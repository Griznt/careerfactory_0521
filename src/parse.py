from csv import DictReader, writer
import datetime
import json
import bisect
import os
from statistics import mean, median

# Used to determine whether two dates belong to the same session or not
# SESSION_WINDOW_VALUES = [None, 730, 360, 240, 120, 60, 48, 36, 24, 12, 8, 6, 4, 2, 1, 0.5] # hours
SESSION_WINDOW_VALUES = [24, 12, 8, 6, 4, 2, 1, 0.5] # hours
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

# def removeWrongSessions(userSessions):
#     if 
#     for session in userSessions:
#         hitAt = session.get('hit_at')
#         orderSum = session.get('order_sum')
#         orderCompletedTimestamp = session.get('orderCompletedTimestamp')
        


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
                    # gets into in case if SESSION_WINDOW is None or timedelta grather than -PREVIOUS_EVENT_DELAY in minutes and lower than SESSION_WINDOW in hour 
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
                                    session[attr].append(value)
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

def saveToJSON(filename, data):
    if not os.path.exists('result'):
        os.makedirs('result')
    with open('result/' + filename + '.json', 'w') as fp:
        json.dump(data, fp)

def saveToCSV(filename, data):
    if not os.path.exists('result'):
        os.makedirs('result')
    dataFile = open('result/' + filename + '.csv', 'w')
    csvWriter = writer(dataFile)
    csvWriter.writerow(['anonymous_id','hit_at','device_type','addressChangeInitiatedTimestamp','productAddedTimestamp','orderCompletedTimestamp','order_sum'])
    for userId in data:
        userSessions = data[userId]
        for session in userSessions:
            row = [userId]
            for key in session:
                item = session[key]
                if type(item) is list:
                    row.append('\r\n'.join(item))
                else:
                    row.append(item)
            csvWriter.writerow(row)

def calculateRevenue(group, data, allUsersCount):
    result = {}
    purchasesCountPerUser = []
    purchasesCount = 0
    payingUsersCount = 0
    allPurchases = []
    revenue = 0
    for userId in data:
        userSessions = data[userId]
        purchases = []
        for session in userSessions:
            userSessionOrdersCost = session.get('order_sum')
            if userSessionOrdersCost:
                # if userSessionOrdersCost not in userRevenue:
                for orderSum in userSessionOrdersCost:
                    if orderSum not in purchases:
                        purchases.append(float(orderSum))
        if not result.get(userId):
            result[userId] = purchases
            purchasesCount += len(purchases)
            purchasesCountPerUser.append(len(purchases))
            for purchase in purchases:
                allPurchases.append(purchase)
                revenue += purchase
            if len(purchases) > 0:
                payingUsersCount += 1

    
    print('--------------------------------------\r\nrevenue')
    print(group,'\r\n',
        'users count:', allUsersCount,'\r\n',
        'paying users count:', payingUsersCount,'\r\n',
        'purchases count:', purchasesCount,'\r\n',
        'purchases count per users from users:', purchasesCount/allUsersCount,'\r\n',
        'purchases per paying user:', mean(purchasesCountPerUser),'\r\n',
        'average purchase value:', mean(allPurchases),'\r\n',
        'median purchase value:', median(allPurchases),
        'ARPU:', revenue/allUsersCount,'\r\n',
        'ARPPU:', revenue/payingUsersCount,
        'total Revenue:', revenue
        )
   
    
    saveToJSON(group + '_revenue', result)
    saveToJSON(group + '_revenue_RESULT',  {
        'users count': allUsersCount,
        'paying users count': payingUsersCount,
        'purchases count': purchasesCount,
        'purchases count per users': purchasesCount/allUsersCount,
        'purchases per paying user': mean(purchasesCountPerUser),
        'average purchase value': mean(allPurchases),
        'median purchase value': median(allPurchases),
        'ARPU': revenue/allUsersCount,
        'ARPPU': revenue/payingUsersCount,
        'payingShare': payingUsersCount/allUsersCount
    })





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

testGroupUniqueUsers = {}
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
        testGroupUniqueUsers[id] = testGroupSessionTimestamps

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

for id, valuesList in testGroupUniqueUsers.items():
    if len(valuesList) > 1:
        testGroupUsersMulti.append(valuesList)
    else:
        value = valuesList[0]
        deviceType = value['device_type']
        if not testGroupUsersByDeviceTypes.get(deviceType):
            testGroupUsersByDeviceTypes[deviceType] = []
        testGroupUsersByDeviceTypes[deviceType].append(value)     

# saveToCSV('controlGroupMulti', controlGroupUsersMulti)
# saveToCSV('testGroupUsersMulti', testGroupUsersMulti)
# saveToCSV('controlGroupUsersByDeviceTypes', controlGroupUsersByDeviceTypes)
# saveToCSV('testGroupUsersByDeviceTypes', testGroupUsersByDeviceTypes)


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
    currentResultName = 'control_group_session_window_' + (SESSION_WINDOW == None and 'unset' or str(SESSION_WINDOW) + 'h')
    
    saveToCSV(currentResultName, result)
    calculateRevenue(currentResultName, result, len(controlGroupUniqueUsers))
    result.clear()
    print('--------------------------------------')

    #------------------
    #------------------
    # test group users
    print('test group unique users', len(testGroupUniqueUsers))
    result = testGroupUniqueUsers
    # get intersection of tguu and users from next step: address change initiated
    result = getNextFunnelStepResult('Address Change Initiated.csv', result, 'addressChangeInitiated')
    saveToJSON('users count on Address Change Initiated test group', result)
    # get intersection of tguu and users from next step: product added
    result = getNextFunnelStepResult('Product Added.csv', result, 'productAdded')
    saveToJSON('users count on Product Added test group', result)
    # product added step
    result = getNextFunnelStepResult('Order Completed.csv', result, 'orderCompleted', ['order_sum'])
    saveToJSON('users count on Order Completed test group', result)
    currentResultName = 'test_group_session_window_' + (SESSION_WINDOW == None and 'unset' or str(SESSION_WINDOW) + 'h')

    saveToCSV(currentResultName, result)
    saveToJSON(currentResultName, result)
    calculateRevenue(currentResultName,result, len(testGroupUniqueUsers))
    
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
print('control group Bounce Rate is:', len(controlGroupBouncedUsers)/len(controlGroupUniqueUsers))
print('--------------------------------------')
testGroupBouncedUsers = testGroupUniqueUsers
for filename in testGroupOrderedStepFilenames:
    getBouncedUsersOnStep(filename, testGroupBouncedUsers)
print('test group bounced users count is', len(testGroupBouncedUsers))
print('test group Bounce Rate is:', len(testGroupBouncedUsers)/len(testGroupUniqueUsers))