import requests
import logging
import pprint
import  json
 
logger=logging.getLogger('kvstore_client')
logger.setLevel(logging.DEBUG)
 
ssl_verify=False
 
class KVStoreClient():
 
        def __init__(self,url,auth_token,app,collection,user='nobody'):
 
                self.url=url
                self.token=auth_token
                self.app=app
                self.collection=collection
                self.user=user
 
        # Not really that useful but
        def getCollectionInfo(self):
                headers = {}
                headers['Authorization'] = 'Splunk %s' % str(self.token)
 
                # This call is special, splunkd *requires* a user of "-"  (as of 6.3.5 / 6.4 anyway)
                wholeurl='/'.join([self.url,'servicesNS','-',self.app,'storage/collections/config',self.collection])
                r=requests.get(wholeurl, headers=headers, verify=ssl_verify, params={'output_mode':'json'})
                #logger.debug(wholeurl)
                #logger.debug(r.request.headers)
                #logger.debug(repr(r))
                #logger.debug(r.text)
                #logger.debug(pprint.pformat(r.json()['entry'][0]))
                return r.json()['entry'][0] # Strip off the outer entry stuff since we should only be returning one
 
        # Returns an array of things
        def get(self, key=None, **kwargs):
                headers = {
                        'Authorization' : 'Splunk %s' % str(self.token),
                        'Content-Type'  : 'application/json'
                }
 
                getparams={
                        #'output_mode' : 'json'
                }
                getparams.update(kwargs)
 
                wholeurl='/'.join([self.url,'servicesNS',self.user,self.app,'storage/collections/data',self.collection])
 
                if key is not None:
                        wholeurl = '/'.join([wholeurl,key])
 
                r=requests.get(wholeurl, headers=headers, verify=ssl_verify, params=getparams)
                #logger.debug('Request returns status %s ' % r.status_code)
                #logger.debug(wholeurl)
                #logger.debug(r.request)
                #logger.debug(repr(r))
                #logger.debug(r.text)
                #logger.debug(pprint.pformat(r.json()))
                q=r.json()
                #logger.debug('finished jsoning')
 
                return q
 
 
        def put(self,data,key=None,**kwargs):
                headers = {
                        'Authorization' : 'Splunk %s' % str(self.token),
                        'Content-Type'  : 'application/json'
                }
 
                getparams={
                        #'output_mode' : 'json'
                }
 
                getparams.update(kwargs)
 
                wholeurl='/'.join([self.url,'servicesNS',self.user,self.app,'storage/collections/data',self.collection])
 
                if key is not None:
                        wholeurl = '/'.join([wholeurl,key])
 
                r=requests.post(wholeurl, headers=headers, verify=ssl_verify, data=json.dumps(data))
 
                logger.debug('Request returns status %s ' % r.status_code)
                logger.debug(wholeurl)
                logger.debug(r.request)
                logger.debug(repr(r))
                logger.debug(r.text)
 
                if r.status_code == requests.codes.ok:
                        return (True,r.status_code)
                else:
                        return (False,r.status_code)

