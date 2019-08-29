import os.path
import pickle

import jmespath
import pandas as pd
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# If modifying these scopes, delete the file token.pickle.
SCOPES = [
    'https://www.googleapis.com/auth/drive.metadata.readonly',
    'https://www.googleapis.com/auth/spreadsheets.readonly',
]


def get_path(*path):
    return os.path.expanduser(os.path.join('~', 'Documents', '.jw', *path))


def cred_path():
    return get_path(
        'client_secret_475112205023-3g9tvb4tc856ne06k3g21hu0imdfve6o.apps.googleusercontent.com.json')


def token_path():
    return get_path('google_token.pickle')


def service(name, version):
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    cred = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(token_path()):
        with open(token_path(), 'rb') as token:
            cred = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(cred_path(), SCOPES)
            cred = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_path(), 'wb') as token:
            pickle.dump(cred, token)

    return build(name, version, credentials=cred)


def get_participants():
    result = []
    page_token = None
    while True:
        response = service('drive', 'v3').files().list(
            q="'1NCR6O8_WqOzCTPkB0dfkxvKCSvHIc27O' in parents",
            pageToken=page_token,
        ).execute()
        result.append(jmespath.search('files[].[id,name,mimeType]', response))
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    return result


def get_sheet(_id, name):
    response = service('sheets', 'v4').spreadsheets().values().get(
        spreadsheetId=_id,
        range='A3:K',
    ).execute()
    print(response.get('values'))
    data = response.get('values', [])
    try:
        int(data[0][0])
        response = service('sheets', 'v4').spreadsheets().values().get(
            spreadsheetId=_id,
            range='B3:L',
        ).execute()
        data = response.get('values', [])
    except ValueError:
        pass
    cols = ['참가자', '이메일', '참가방법', '직군', '사전취소',
            '입금액', '환불액', '출석체크', '지각', '조퇴', '비고']
    df = pd.DataFrame(columns=cols)
    for i, row in enumerate(data):
        df.loc[i] = row + ([''] * (len(cols) - len(row)))
    df['sheet'] = name
    return df


if __name__ == '__main__':
    dodo_list = get_participants()

    result = []
    for sheet_id, name, mime_type in dodo_list[0]:
        print(name, mime_type)
        if not mime_type.endswith('spreadsheet'):
            continue
        data = get_sheet(sheet_id, name)
        result.append(data)
    res = pd.concat(result, sort=False)
    res.to_csv('dodo_participants.csv', index=False)
