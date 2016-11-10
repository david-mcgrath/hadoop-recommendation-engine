import requests, StringIO, pandas as pd, json, re

cred_ratings = {
  'auth_url':'https://identity.open.softlayer.com',
  'project':'object_storage_ee9ca24f_81be_4ef0_a658_314e27fad09a',
  'project_id':'8b9bd11cb9294148b63b784fc254f34e',
  'region':'dallas',
  'user_id':'c0a11a5e37f94cb4a6778b1445761fc9',
  'domain_id':'ed55f698827b43b59ebc0d2dddb36fba',
  'domain_name':'1154171',
  'username':'admin_ab95c493a010d6c326ad7e6d2e6dd047c44a4b54',
  'password':"""r8#.b7~cVd).kXeR""",
  'filename':'ratings.csv',
  'container':'notebooks',
  'tenantId':'s226-3bfa7401ec1542-d9ec086b762a'
}

cred_movies = {
  'auth_url':'https://identity.open.softlayer.com',
  'project':'object_storage_ee9ca24f_81be_4ef0_a658_314e27fad09a',
  'project_id':'8b9bd11cb9294148b63b784fc254f34e',
  'region':'dallas',
  'user_id':'c0a11a5e37f94cb4a6778b1445761fc9',
  'domain_id':'ed55f698827b43b59ebc0d2dddb36fba',
  'domain_name':'1154171',
  'username':'admin_ab95c493a010d6c326ad7e6d2e6dd047c44a4b54',
  'password':"""r8#.b7~cVd).kXeR""",
  'filename':'movies.csv',
  'container':'notebooks',
  'tenantId':'s226-3bfa7401ec1542-d9ec086b762a'
}

# FROM IBM BLUEMIX NOTEBOOKS TUTORIAL
def get_file_content(credentials):
    """For given credentials, this functions returns a StringIO object containing the file content."""

    url1 = ''.join([credentials['auth_url'], '/v3/auth/tokens'])
    data = {'auth': {'identity': {'methods': ['password'],
            'password': {'user': {'name': credentials['username'],'domain': {'id': credentials['domain_id']},
            'password': credentials['password']}}}}}
    headers1 = {'Content-Type': 'application/json'}
    resp1 = requests.post(url=url1, data=json.dumps(data), headers=headers1)
    resp1_body = resp1.json()
    for e1 in resp1_body['token']['catalog']:
        if(e1['type']=='object-store'):
            for e2 in e1['endpoints']:
                if(e2['interface']=='public'and e2['region']==credentials['region']):
                    url2 = ''.join([e2['url'],'/', credentials['container'], '/', credentials['filename']])
    s_subject_token = resp1.headers['x-subject-token']
    headers2 = {'X-Auth-Token': s_subject_token, 'accept': 'application/json'}
    resp2 = requests.get(url=url2, headers=headers2)
    return resp2.content

def get_ratings():
    contents = [ l.split(",") for l in get_file_content( cred_ratings ).splitlines() ]
    return contents

def get_movies():
    contents = [ l.split(",") for l in get_file_content( cred_movies ).splitlines() ]
    return contents
