from __future__ import print_function
import os
import glob
import datetime 
from pdb import set_trace as stop 
#BaseSpace API imports
import BaseSpacePy
from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from BaseSpacePy.api.BaseSpaceAPI import QueryParameters

def pathFromFile(fn, myAPI):
    '''
    input: basespace file object, basespace API instance 
    return: save path, based on file location in amazon cloud
    attempts to retain as much path information as possible
    '''    
    url = fn.getFileUrl(myAPI) # fn.getFileS3metadata(myAPI)['url'] <-- can throw a urllib2.HTTPError if the file is empty 
    savePath = url.split('amazonaws.com/')[1].split('?AWSA')[0]
    savePath = "/".join(savePath.split('/')[1:-1])
    # stop()
    return savePath

def stringsToBSObj(BSlist, USRlist):
    '''
    input: a list of basespace objects, and a list of string names to select on from that list
    output: list of basespace objects, from the given list of objects, that matched the list of string identifiers
    
    Ex. [Proj1, Proj2, Proj3], ["Proj1", "Proj3"]
    returns [Proj1, Proj3] as a list of basespace project class instances
    ''' 
    # just in case the user-list was a mix of string and basespace objects
    outlist = []
    toClean = []
    for item in USRlist:
        if type(item) in [BaseSpacePy.model.Sample.Sample, BaseSpacePy.model.Project.Project]:
            # no need to clean this
            outlist.append(item)
        else:
            # will need to convert this string to a basespace object
            toClean.append(item)
            
    # readability > concision
    # [outlist.append(item) for item in BSlist if str(item) in toClean]
    for item in BSlist:
        if str(item) in toClean:
            outlist.append(item)
    return outlist
    
def humanFormat(num):
    '''
    input: number of bytes as an int 
    output: string of corresponding, human-readable format 
    '''
    if int(num/(1<<40)):
        return "{0:.2f}".format(float(num)/(1<<40)) + "TB"
    if int(num/(1<<30)):
        return "{0:.2f}".format(float(num)/(1<<30)) + "GB"
    if int(num/(1<<20)):
        return "{0:.2f}".format(float(num)/(1<<20)) + "MB"
    if int(num/(1<<10)):
        return "{0:.2f}".format(float(num)/(1<<10)) + "KB"
    else:
        return str(num)
        
def pickSomething(selectionType, potentialSelectionsList):
    '''
    input: type of items from which user will be selecting, a list of basespace objects corresponding to that selector
    output: a list of the selected basespace objects
    
    prompts users to pick one or more items from a list of basespace objects. 
    selection is done by entering a list of ints which correspond to an object
    
    Ex. selectionType = 'project', potentialSelectionsList = [Project1, Project2, Proj3]
    <user prompted for raw input; enters '1 2' for example>
    output = [Project2, Proj3]
    '''
    itemDict = {}
    for i, item in enumerate(potentialSelectionsList):
        itemDict[i] = item 
    for i,item in itemDict.items():
        print("[{0}]\t{1}".format(i, item))
    selection = raw_input("select a " + selectionType + " : ")
    if selection == "":
        # no selection assumes all selected
        outList = itemDict.values()
    else:
        outList = []
        for picked in selection.split():
            if int(picked) not in itemDict.keys():
                print("no " +selectionType+" with id #" + str(picked))
                continue
            outList.append(itemDict[int(picked)])
    return outList        
    
def fileExists(pathToFn, BSfn):
    '''
    input: a string of intended download location (path and filename) and a basespace file object
    output: boolean 
    
    determines whether the file already exists on disk, comparing file name and size
    todo: compare date of creation? 
          establish preference by size/date rather than downloading all instances of a file 
    '''
    for LOCfn in glob.glob(pathToFn+"*"):
        # if os.path.exists(LOCfn):    
            # a file by this name exists on disk
        if os.path.getsize(LOCfn) == BSfn.Size:
            # the file on disk is the same size as the file in cloud
            return True
    return False    
    
def pullMetadata(bsobj):
    '''
    input: a basespace object
    output: a dictionary of metadatas 
    
    leverages the fact that basespace objects have a consistent scheme for metadata.
    extracts all present metadata and returns in in key,value format as a dictionary
    '''     
    validMetadataTypes = [str, int, datetime.datetime ]
    metadata = dict()
    for key,value in bsobj.__dict__.items():
        if type(value) in validMetadataTypes and not key.startswith('__'):
            metadata[key] = value
    for key in dir(bsobj):
        value = getattr(bsobj,key)
        if type(value) in validMetadataTypes and not key.startswith('__') and key not in metadata.keys():
            # good metadata 
            metadata[key] = value
    if hasattr(bsobj,'swaggerTypes'):
        for key in getattr(bsobj, 'swaggerTypes').keys():
            if hasattr(bsobj, key):
                value = getattr(bsobj,key)
                if type(value) in validMetadataTypes and not key.startswith('__') and key not in metadata.keys():
                    metadata[key] = value
    return metadata 
    
def warning(message):
    print("WARNING!")
    print(message)
    print()