#BaseSpace API imports
from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from BaseSpacePy.api.BaseSpaceAPI import QueryParameters

def pathFromFile(fn, myAPI):
    '''
    take a basespace file object and turn it into an appropriate path
    '''
    url = fn.getFileS3metadata(myAPI)['url']
    savePath = url.split('amazonaws.com/')[1].split('?AWSA')[0]
    savePath = "/".join(savePath.split('/')[1:-1])
    return savePath
