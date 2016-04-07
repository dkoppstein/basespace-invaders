#BaseSpace API imports
from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from BaseSpacePy.api.BaseSpaceAPI import QueryParameters

def pathFromFile(fn, myAPI):
    '''
    input: basespace file object, basespace API instance 
    return: save path, based on file location in amazon cloud
    attempts to retain as much path information as possible
    '''
    url = fn.getFileS3metadata(myAPI)['url']
    savePath = url.split('amazonaws.com/')[1].split('?AWSA')[0]
    savePath = "/".join(savePath.split('/')[1:-1])
    return savePath

def stringSampsToSampSamps(project, myAPI, samples, qp=QueryParameters.QueryParameters({'Limit':1024})):
    '''
    input: basespace project object, basespace API instance, string list of sample identifiers, optional basespace query params object
    output: list of basespace sample objects, from the given project, that matched the list of string identifiers
    ''' 
    return [x for x in project.getSamples(myAPI, qp) if str(x) in samples]

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
        outList = itemDict.items()
    else:
        outList = []
        for picked in selection.split():
            if int(picked) not in itemDict.keys():
                print("no " +selectionType+" with id #" + str(picked))
                continue
            outList.append(itemDict[int(picked)])
    return outList        