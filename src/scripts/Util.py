from __future__ import print_function
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
    url = fn.getFileS3metadata(myAPI)['url']
    savePath = url.split('amazonaws.com/')[1].split('?AWSA')[0]
    savePath = "/".join(savePath.split('/')[1:-1])
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
        outList = itemDict.items()
    else:
        outList = []
        for picked in selection.split():
            if int(picked) not in itemDict.keys():
                print("no " +selectionType+" with id #" + str(picked))
                continue
            outList.append(itemDict[int(picked)])
    return outList        
    
def warning(message):
    print("WARNING!")
    print(message)
    print()