#coding: utf-8

import random

import requests


def main():
    char_set = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'
    for _ in xrange(1000000):
        key = ''.join(random.sample(char_set, 20))
        value = ''.join(random.sample(char_set, 30))
        payload = {'key': key, 'value': value}
        
        resp = requests.post('http://127.0.0.1:8799/leveldb/', data=payload)
        
        print resp.status_code
        if resp.status_code == 200:
            resp_content = resp.json()
            print resp_content['Status'], resp_content['Msg']

if __name__ == '__main__':
    main()
