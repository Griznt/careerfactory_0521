from csv import DictReader, writer
import datetime
import json
import bisect

# TODO: It's needed to add folder creation

# Used to determine whether two dates belong to the same session or not
SESSION_WINDOW_VALUES = [None, 730, 360, 240, 120, 60, 48, 36, 24, 12, 8, 6, 4, 2, 1, 0.5] # hours
PREVIOUS_EVENT_DELAY = 10 #minutes

def parseDate(date):
    format = '%Y-%m-%d %H:%M:%S.%f UTC'
    try:
        # fix for wrong date format without milliseconds (example: 2020-12-01 14:32:04 UTC)
        if '.' not in date:
            format = '%Y-%m-%d %H:%M:%S UTC'
        return datetime.datetime.strptime(date, format)
    except ValueError:
        return None

# Load new csv file and for each row search userSessions from previous step data.
# Returns new data for current step of funnel
def getNextFunnelStepResult(filename, previousStepData, _timestampName, attributes = False):
    timestampName = _timestampName + 'Timestamp'
    newData = {}
    with open('ext/'+ filename, 'r') as read_obj:
        csv_dict_reader = DictReader(read_obj)
        for row in csv_dict_reader:
            id = row['anonymous_id']
            userSessions = previousStepData.get(id)
            if userSessions:
                # checking current intersection with users sessions:
                currentEventDate = row['timestamp']
                for session in userSessions:
                    sessionTime = session.get('hit_at')
                    timedelta =  parseDate(currentEventDate) - parseDate(sessionTime) 
                    # gets into in case if SESSION_WINDOW is None or timedelta grather than -20 minutes and lower than SESSION_WINDOW param 
                    if not SESSION_WINDOW or (-1 * datetime.timedelta(minutes=PREVIOUS_EVENT_DELAY)) < timedelta < datetime.timedelta(hours=SESSION_WINDOW):
                        if not session.get(timestampName):
                            session[timestampName] = []
                        if currentEventDate not in session[timestampName]:
                            # insert new value to ordered list
                            bisect.insort(session[timestampName], currentEventDate)
                        if attributes:
                            for attr in attributes:
                                if not session.get(attr):
                                    session[attr] = []
                                    value = row[attr]
                                    if value not in session[attr]:
                                        # insert new value to ordered list
                                        bisect.insort(session[attr], value)
                        if not newData.get(id):
                            newData[id] = []
                        
                        if session not in newData[id]:
                            newData[id].append(session)
    print('users count on step ' + _timestampName, len(newData))
    return newData

def getBouncedUsersOnStep(filename,  firstStepUsers):
    array = []
    with open('ext/'+ filename + '.csv', 'r') as read_obj:
        csv_dict_reader = DictReader(read_obj)
        for row in csv_dict_reader:
            id = row['anonymous_id']
            if firstStepUsers.get(id):
                del firstStepUsers[id]
                if id not in array:
                    array.append(id)
    print('users followed on next step: ', len(array), '\r\nusers count, who didn\'t go to step \"', filename, '\" is', len(firstStepUsers))
    # return firstStepUsers 

def saveToFile(filename, data):
    with open('result/' + filename, 'w') as fp:
        json.dump(data, fp)

# def iterateOverDict(data):
#     jsonArray = []
#     for key in data:
#         value = data[key]
#         print('key', key, '\r\nvalue', value)
#         row = {}
#         print('type(value)', type(value), '\r\ntype(value) is dict', type(value) is dict, '\r\ntype(value) is list', type(value) is list ,'\r\n', value)
#         if type(value) is dict:
#             iterateOverDict(data)
#         elif type(value) is list:
#             row[key] = '\n'.join(value)
#         else:
#             row[key] = value
#         jsonArray.append(row)
#     return jsonArray

def saveToCSV(filename, data):
    jsonArray = []
    for userData in data:
        print(type(userData))
        for key in userData:
            item = userData[key]
            row = {}
            if type(item) is list:
                row[key] = '\n'.join(item)
            else:
                row[key] = item
            jsonArray.append(row)
    saveToFile(filename, jsonArray)





# get unique users
uniqueUsers = {}
with open('ext/AB Test Hit.csv', 'r') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        id = row['anonymous_id']
        if not id in uniqueUsers:
            uniqueUsers[id] = []
        uniqueUsers[id].append({'group': row['group'] , 'hit_at': row['hit_at'], 'device_type': row['device_type']})

# get users who is in mixed groups

CONTROL_GROUP_NAME = 'default'
TEST_GROUP_NAME = 'address_first'

testGroupUniuqueUsers = {}
controlGroupUniqueUsers = {}

mixedGroup = []

for id, sessions in uniqueUsers.items():

    controlGroupSessionTimestamps = []
    testGroupSessionTimestamps = []
    for session in sessions:
        _session = {"hit_at": session.get('hit_at'), "device_type": session.get('device_type')};
        if session.get('group') == CONTROL_GROUP_NAME:
            controlGroupSessionTimestamps.append(_session)
        if session.get('group') == TEST_GROUP_NAME:
            testGroupSessionTimestamps.append(_session)

    if len(controlGroupSessionTimestamps) > 0 and len(testGroupSessionTimestamps) > 0:
        mixedGroup.append(id)
    elif len(controlGroupSessionTimestamps) > 0:
        controlGroupUniqueUsers[id] = controlGroupSessionTimestamps
    elif len(testGroupSessionTimestamps) > 0:
        testGroupUniuqueUsers[id] = testGroupSessionTimestamps

print(
'total unique users', len(uniqueUsers),
'\r\nusers in both groups:', len(mixedGroup),
'\r\nunique without mixed users:', len(uniqueUsers) - len(mixedGroup))
uniqueUsers.clear()


print('--------------------------------------')
print('users devices distribution')

controlGroupUsersByDeviceTypes = {}
controlGroupUsersMulti = []
testGroupUsersByDeviceTypes = {}
testGroupUsersMulti = []

for id, valuesList in controlGroupUniqueUsers.items():
    if len(valuesList) > 1:
        controlGroupUsersMulti.append(valuesList)
    else:
        value = valuesList[0]
        deviceType = value['device_type']
        if not controlGroupUsersByDeviceTypes.get(deviceType):
            controlGroupUsersByDeviceTypes[deviceType] = []
        controlGroupUsersByDeviceTypes[deviceType].append(value) 

for id, valuesList in testGroupUniuqueUsers.items():
    if len(valuesList) > 1:
        testGroupUsersMulti.append(valuesList)
    else:
        value = valuesList[0]
        deviceType = value['device_type']
        if not testGroupUsersByDeviceTypes.get(deviceType):
            testGroupUsersByDeviceTypes[deviceType] = []
        testGroupUsersByDeviceTypes[deviceType].append(value)     

# saveToCSV('controlGroupMulti.json', controlGroupUsersMulti)
# saveToCSV('testGroupUsersMulti.json', testGroupUsersMulti)
# saveToCSV('controlGroupUsersByDeviceTypes.json', controlGroupUsersByDeviceTypes)
# saveToCSV('testGroupUsersByDeviceTypes.json', testGroupUsersByDeviceTypes)


for SESSION_WINDOW in SESSION_WINDOW_VALUES:

    print('--------------------------------------')
    print('session window is', SESSION_WINDOW == None and 'unset' or str(SESSION_WINDOW) + ' hours')
    # control group users
    print('--------------------------------------')
    print('control group unique users', len(controlGroupUniqueUsers))
    result = controlGroupUniqueUsers
    result = getNextFunnelStepResult('Address Change Initiated.csv', result, 'addressChangeInitiated')
    # get intersection of cguu and users from next step: product added
    result = getNextFunnelStepResult('Product Added.csv', result, 'productAdded')
    # get intersection of cguu and users from next step: order completed
    result = getNextFunnelStepResult('Order Completed.csv', result, 'orderCompleted', ['order_sum'])
    saveToCSV('control_group_session_window_' + (SESSION_WINDOW == None and 'unset' or str(SESSION_WINDOW) + 'h') + '.json', result)
    result.clear()
    print('--------------------------------------')

    #------------------
    #------------------
    # test group users
    print('test group unique users', len(testGroupUniuqueUsers))
    result = testGroupUniuqueUsers
    # get intersection of tguu and users from next step: address change initiated
    result = getNextFunnelStepResult('Address Change Initiated.csv', result, 'addressChangeInitiated')
    # get intersection of tguu and users from next step: product added
    result = getNextFunnelStepResult('Product Added.csv', result, 'productAdded')
    # product added step
    result = getNextFunnelStepResult('Order Completed.csv', result, 'orderCompleted', ['order_sum'])
    saveToCSV('test_group_session_window_' + (SESSION_WINDOW == None and 'unset' or str(SESSION_WINDOW) + 'h') + '.json', result)
    result.clear()

print('\r\n')
print('--------------------------------------')
# bounce rate calculation:
# Bounced users are users who exit after viewing only one page.
# For each group it's first page: landing, and event: Landing Viewed.
# So, lets calculate it!

controlGroupOrderedStepFilenames = [
    'Main Page Viewed', 
    'Address Change Initiated',
    'Address Not In Delivery Zone',
    # 'Shop Selection Started', 
    # 'Shop Selected',  
    'Product Added',
    'Add To Cart Clicked', 
    'Order Completed']

testGroupOrderedStepFilenames = [
    'Address Change Initiated',
    'Address Not In Delivery Zone',
    'Shop Selection Started', 
    'Shop Selected',  
    'Main Page Viewed', 
    'Product Added',
    'Add To Cart Clicked', 
    'Order Completed']

print('bounce rate calculation')
print('--------------------------------------')
controlGroupBouncedUsers = controlGroupUniqueUsers
for filename in controlGroupOrderedStepFilenames:
    getBouncedUsersOnStep(filename, controlGroupBouncedUsers)
print('control group bounced users count is', len(controlGroupBouncedUsers))
print('--------------------------------------')
testGroupBouncedUsers = testGroupUniuqueUsers
for filename in testGroupOrderedStepFilenames:
    getBouncedUsersOnStep(filename, testGroupBouncedUsers)
print('test group bounced users count is', len(testGroupBouncedUsers))