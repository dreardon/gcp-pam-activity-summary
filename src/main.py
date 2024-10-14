import json
import base64
import vertexai
import os
import markdown
import requests
from vertexai.generative_models import GenerativeModel
from google.cloud import logging
from google.cloud import privilegedaccessmanager_v1
from google.cloud.logging import DESCENDING
from google.cloud.asset_v1 import AssetServiceClient, SearchAllResourcesRequest
from flask import Flask, request
import google
import google.oauth2.credentials
from google.auth import compute_engine
import google.auth.transport.requests

app = Flask(__name__)

summary_project_id = os.environ.get("PROJECT_ID", "Project ID not set in Cloud Run Function environment variable")
region = os.environ.get("REGION", "Region not set in Cloud Run Function environment variable")
summary_recipient = os.environ.get("SUMMARY_RECIPIENT", "Summary recipient not set in Cloud Run Function environment variable")

@app.route("/", methods=["POST"])
def index(*args, **kwargs):
    envelope = request.get_json()
    if not envelope:
        msg = "No Pub/Sub message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "invalid Pub/Sub message format"
        return f"Bad Request: {msg}", 400
    pubsub_message = envelope["message"]
    
    if isinstance(pubsub_message, dict) and "data" in pubsub_message:
        name = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
        message = json.loads(name)
        grant_message = message['protoPayload']['resourceName']
    
    grant = get_pam_grants(grant_message)
    grantee = grant['requester']
    activity = get_pam_activities(grant)
    summary = generate_summary(summary_project_id, activity)
    send_notification(grantee,summary)

    return ("PAM Grant Summary Processed Successfully", 200)

def get_pam_grants(grant_message):
    pam_client = privilegedaccessmanager_v1.PrivilegedAccessManagerClient()
    
    result = pam_client.get_grant(name=grant_message)

    grant = {}
    grant['name'] = result.name
    grant['requester'] = result.requester
    grant['duration_in_seconds'] = result.requested_duration.seconds
    grant['state'] = result.state.name
    grant['justification'] = result.justification.unstructured_justification or ""
    grant['roles'] = result.privileged_access.gcp_iam_access.role_bindings[0].role or ""
    grant['roles_scope'] = result.privileged_access.gcp_iam_access.resource or ""
    grant['start_time'] = result.audit_trail.access_grant_time.isoformat() or ""
    grant['end_time'] = result.audit_trail.access_remove_time.isoformat() or ""
    return grant

def get_pam_activities(grant):
    current_grant = grant
    current_grant['activities'] = []
    start_datetime=grant.get('start_time')
    end_datetime=grant.get('end_time', "")
    FILTER = '''
        protoPayload.authenticationInfo.principalEmail={0} AND
        timestamp>="{1}" AND timestamp<="{2}"
        '''.format(grant['requester'],start_datetime,end_datetime)

    print('Log Filter Used:', FILTER)

    client = AssetServiceClient()
    grant_scope = grant['roles_scope'].replace('//cloudresourcemanager.googleapis.com/','')
    request = SearchAllResourcesRequest(
        scope=grant_scope,
        asset_types=[
            "cloudresourcemanager.googleapis.com/Project",
        ],
        query="state:ACTIVE",
    )

    paged_results = client.search_all_resources(request=request)

    for response in paged_results:
        project_id = response.name.split("/")[4]
        client = logging.Client(project=project_id)
        iterator = client.list_entries(filter_=FILTER, order_by=DESCENDING)
        for entry in iterator:
            entry = entry.to_api_repr()
            activity = {}
            activity["project_name"] = response.display_name
            activity["project_id"] = project_id
            activity["service_name"] = entry['protoPayload']['serviceName']
            activity["method_name"] = entry['protoPayload']['methodName']
            activity["resource_name"] = entry.get('protoPayload', {}).get('resourceName', "")
            activity["timestamp"] = entry['timestamp']
            #activity["raw"] = entry
            current_grant["activities"].append(activity)

    return current_grant

def generate_summary(summary_project_id, activity):

    vertexai.init(project=summary_project_id, location="us-central1")
    
    model = GenerativeModel(
        "gemini-1.5-flash-002",
    )
    
    generation_config = {
        "max_output_tokens": 8192,
        "temperature": 1,
        "top_p": 0.95,
    }

    # Prompt tokens count
    response = model.count_tokens(json.dumps(activity))
    print(f"Prompt Token Count: {response.total_tokens}")
    print(f"Prompt Character Count: {response.total_billable_characters}")

    response = model.generate_content(
        json.dumps(activity),
        generation_config=generation_config,
    )

    return response.text

def send_notification(grantee, summary):
    def token_from_metadata_server():
        request = google.auth.transport.requests.Request()
        credentials = compute_engine.Credentials()
        credentials.refresh(request)
        return credentials.token
    
    access_token = token_from_metadata_server()
    api_endpoint = 'api_trigger/pam-summary-email'
    pam_user = grantee
    app_int_endpoint = 'https://{0}-integrations.googleapis.com/v1/projects/{1}/locations/{0}/integrations/-:execute'.format(region,summary_project_id)
    app_int_config = {"trigger_id": api_endpoint,
        "input_parameters": { "recipient": {"string_value": summary_recipient }, 
            "pam_user" : {"string_value" : pam_user}, 
            "email_content" : {"string_value" : markdown.markdown(summary)}
            }
        }
    
    headers = {"Authorization": "Bearer " + access_token, "Content-Type": "application/json"} 
    response = requests.post(app_int_endpoint, json=app_int_config, headers=headers)
    print('Sent Notification', response)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))