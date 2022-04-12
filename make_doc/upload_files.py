from distutils.command.upload import upload
import requests
import os
import base64
import json
from os import walk
def file_upload(repo,owner,token,file_path,git_path,commit,branch):
        text_file = open(file_path, "rb")        
        content = text_file.read()
        content = base64.b64encode(content).decode('ascii')
        text_file.close()


        headers =  {
            'Authorization': f'Token {token}',
            'Content-Type': 'application/vnd.github.v3+json'
        }

        print(f'https://api.github.com/repos/{owner}/{repo}/contents/{git_path}')

        r = requests.get(f'https://api.github.com/repos/{owner}/{repo}/contents/{git_path}', headers=headers)
        
        print(r.status_code)
        
        if r.status_code == 404:
            print('file created')
            data = {'message': commit, 'content': content, 'branch': branch}
            res = requests.put(f'https://api.github.com/repos/{owner}/{repo}/contents/{git_path}', headers=headers ,data=json.dumps(data))
            print(res.json())
        elif r.status_code == 200:
            print('file updated')
            file_inf = r.json()
            sha = file_inf['sha']
            data = {'message': commit, 'content': content, 'branch': branch,'sha':sha}
            res = requests.put(f'https://api.github.com/repos/{owner}/{repo}/contents/{git_path}', headers=headers ,data=json.dumps(data))
            print(res.json())
        else:
            print('could not upload file')
            print(r.json())


def dir_to_upload(dir):
    print(os.getcwd())
    for path, subdirs, files in walk(dir):
        for name in files:
            file_path = path+'/'+name
            git_path = file_path.replace('_build/','')
            print(file_path)
            print(git_path)
            file_upload('for_testing', 'Vito39', os.environ['TOKEN'], file_path, git_path, 'to test','main')

if __name__ == "__main__":
    dir_to_upload('_build/markdown')