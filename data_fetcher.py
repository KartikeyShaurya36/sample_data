import json
import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account

SERVICE_ACCOUNT_PATH = "service_key.json"
PROJECT_ID = "rejara"
DATABASE_ID = "rejara-dev-db"
ROOT_COLLECTION = "ai_conversation"

SCOPES = ["https://www.googleapis.com/auth/datastore"]

def get_access_token():
    print("debug1")
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_PATH, scopes=SCOPES
    )
    credentials.refresh(Request())
    return credentials.token

def parse_document_data(doc):
    print("debug2")
    doc_id = doc["name"].split("/")[-1]
    fields = doc.get("fields", {})
    data = {"id": doc_id}
    
    for key, val in fields.items():
        data[key] = list(val.values())[0]
    
    return data

def list_subcollections(doc_path, token):
    print("debug3")
    url = f"https://firestore.googleapis.com/v1/projects/{PROJECT_ID}/databases/{DATABASE_ID}/documents/{doc_path}:listCollectionIds"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        return response.json().get("collectionIds", [])
    except requests.RequestException:
        return []

def fetch_documents_with_subcollections(path, token, depth=0):
    print("debug4")
    full_path = f"projects/{PROJECT_ID}/databases/{DATABASE_ID}/documents/{path}"
    url = f"https://firestore.googleapis.com/v1/{full_path}"
    headers = {"Authorization": f"Bearer {token}"}
    
    documents = []
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        docs = response.json().get("documents", [])
        
        for doc in docs:
            doc_data = parse_document_data(doc)
            doc_path = doc["name"].replace(f"projects/{PROJECT_ID}/databases/{DATABASE_ID}/documents/", "")
            
            subcollections = list_subcollections(doc_path, token)
            for subcol in subcollections:
                sub_docs = fetch_documents_with_subcollections(f"{doc_path}/{subcol}", token, depth + 1)
                doc_data[subcol] = sub_docs
                
            documents.append(doc_data)
    except requests.RequestException as err:
        print(f"Error fetching {path}: {err}")
    
    return documents

if __name__ == "__main__":
    token = get_access_token()
    data = fetch_documents_with_subcollections(ROOT_COLLECTION, token)
    file_name = f"{ROOT_COLLECTION}_deep_full_export.json"
    
    with open(file_name, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"✅ Exported deeply nested Firestore data to {file_name}")
