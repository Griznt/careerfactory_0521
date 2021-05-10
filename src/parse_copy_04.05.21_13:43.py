import pandas as pd
from csv import DictReader, writer
import datetime
import numpy as np
import json

# AB_TEST_HIT = pd.read_csv('ext/' + 'AB Test Hit.csv')
# ADD_TO_CART_CLICKED = pd.read_csv('ext/' + 'Add To Cart Clicked.csv')
# ADDRESS_CHANGE_INITIATED = pd.read_csv('ext/' + 'Address Change Initiated.csv')
# ADDRESS_NOT_IN_DELIVERY_ZONE = pd.read_csv('ext/' + 'Address Not In Delivery Zone.csv')
# LANDING_VIEWED = pd.read_csv('ext/' + 'Landing Viewed.csv')
# MAIN_PAGE_VIEWED = pd.read_csv('ext/' + 'Main Page Viewed.csv')
# ORDER_COMPLETED = pd.read_csv('ext/' + 'Order Completed.csv')
# PRODUCT_ADDED = pd.read_csv('ext/' + 'Product Added.csv')
# SHOP_SELECTION_STARTED = pd.read_csv('ext/' + 'Shop Selection Started.csv')
# SHOP_SELECTED = pd.read_csv('ext/' + 'Shop Selected.csv')

# Used to determine whether two dates belong to the same session or not
ONE_SESSION_LENGTH = 12

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
def getNextFunnelStepResult(filename, previousStepData, _timestampName, logging = False):
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
                #{'hit_at': '2020-12-05 10:01:51.373 UTC', 'landingViewedTimestamp': ['2020-12-05 10:01:51.305 UTC']}
                for session in userSessions:
                    sessionTime = session.get('hit_at')
                    timedelta = abs( parseDate(currentEventDate) - parseDate(sessionTime) )
                    if timedelta < datetime.timedelta(hours=ONE_SESSION_LENGTH):
                        if not session.get(timestampName):
                            session[timestampName] = []
                        # if timedelta < datetime.timedelta(hours=1):
                        if currentEventDate not in session[timestampName]:
                            session[timestampName].append(currentEventDate)

                        if logging == True:
                            print('id', id, 'session: ', session)

                        if not newData.get(id):
                            newData[id] = []
                        
                        if session not in newData[id]:
                            newData[id].append(session)
    print('users count on step ' + _timestampName, len(newData))
    return newData

def saveToFile(fileName, data):
    # with open('result/test-group.csv', 'w', newline='') as file:
    #     _writer = writer(file)
    #     for key, value in testGroupUsersOnProductAddedStep.items():
    #         _writer.writerow([key, value])

    with open('result/' + fileName, 'w') as fp:
        json.dump(data, fp)

# get unique users
uniqueUsers = {}
with open('ext/AB Test Hit.csv', 'r') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        id = row['anonymous_id']
        if not id in uniqueUsers:
            uniqueUsers[id] = []
        uniqueUsers[id].append({'group': row['group'] , 'hit_at': row['hit_at']})

# get users who is in mixed groups

CONTROL_GROUP_NAME = 'default'
TEST_GROUP_NAME = 'address_first'

testGroupUniuqueUsers = {}
controlGroupUniqueUsers = {}

print('Unique users length before: ', len(uniqueUsers))

mixedGroup = []

for id, sessions in uniqueUsers.items():

    controlGroupSessionTimestamps = []
    testGroupSessionTimestamps = []
    for session in sessions:
        _session = {"hit_at": session.get('hit_at')};
        if session.get('group') == 'default':
            controlGroupSessionTimestamps.append(_session)
        if session.get('group') == 'address_first':
            testGroupSessionTimestamps.append(_session)

    if len(controlGroupSessionTimestamps) > 0 and len(testGroupSessionTimestamps) > 0:
        mixedGroup.append(id)
    elif len(controlGroupSessionTimestamps) > 0:
        controlGroupUniqueUsers[id] = controlGroupSessionTimestamps
    elif len(testGroupSessionTimestamps) > 0:
        testGroupUniuqueUsers[id] = testGroupSessionTimestamps

print('ready.',
'\r\nsession window is', ONE_SESSION_LENGTH, 'hours'
'\r\nuniqueUsersLength:', len(uniqueUsers),
'\r\ncontrol group len:', len(controlGroupUniqueUsers),
'\r\ntest group len:', len(testGroupUniuqueUsers),
'\r\nmixed users:', len(mixedGroup))

# get unique users substract mixed users
print('unique users substact users in both groups:', len(uniqueUsers) - len(mixedGroup))
uniqueUsers.clear()

# 1 control group users
print('--------------------------------------')
print('control group unique users', len(controlGroupUniqueUsers))

# 2 get intersection of uucg and users from next step: landing viewed
controlGroupUsersOnLandingViewedStep = getNextFunnelStepResult('Landing Viewed.csv', controlGroupUniqueUsers, 'landingViewed')
controlGroupUniqueUsers.clear()

# {'id': '05cb2fdc-ec9d-4e1e-b2eb-d9db3926c324', 'hit_at': ['2020-11-26 12:54:18.386 UTC', '2020-11-27 08:42:49.059 UTC'], 'landingViewedTimestamp': '2020-11-27 07:46:45.84 UTC'}
# 2020-11-26 12:54:18.448 UTC,05cb2fdc-ec9d-4e1e-b2eb-d9db3926c324
# 2020-11-27 07:46:45.84 UTC,05cb2fdc-ec9d-4e1e-b2eb-d9db3926c324            

#------------------
# 3 get intersection of uucg and users from next step: main page viewed

controlGroupUsersOnMainPageViewedStep = getNextFunnelStepResult('Main Page Viewed.csv', controlGroupUsersOnLandingViewedStep, 'mainPageViewed')
controlGroupUsersOnLandingViewedStep.clear()
#------------------
# 4 get intersection of uucg and users from next step: address change initiated

controlGroupUsersOnAddressChangeInitiatedStep = getNextFunnelStepResult('Address Change Initiated.csv', controlGroupUsersOnMainPageViewedStep, 'addressChangeInitiated')
controlGroupUsersOnMainPageViewedStep.clear()
#------------------
# 5 get intersection of uucg and users from next step: main page viewed

controlGroupUsersOnMainPageViewed2Step = getNextFunnelStepResult('Main Page Viewed.csv', controlGroupUsersOnAddressChangeInitiatedStep, 'mainPageViewed2')
controlGroupUsersOnAddressChangeInitiatedStep.clear()
#------------------
#  get intersection of uucg and users from next step: add to cart

# controlGroupUsersOnAddToCartStep = getNextFunnelStepResult('Add To Cart Clicked.csv', controlGroupUsersOnMainPageViewed2Step, 'addToCartClicked')
# controlGroupUsersOnMainPageViewed2Step.clear()
#------------------
# 6 get intersection of uucg and users from next step: product added
# instead of next
controlGroupUsersOnProductAddedStep = getNextFunnelStepResult('Product Added.csv', controlGroupUsersOnMainPageViewed2Step, 'productAdded')
saveToFile('control-group.json', controlGroupUsersOnProductAddedStep)
controlGroupUsersOnMainPageViewed2Step.clear()
controlGroupUsersOnProductAddedStep.clear()

# controlGroupUsersOnProductAddedStep = getNextFunnelStepResult('Product Added.csv', controlGroupUsersOnAddToCartStep, 'productAdded')
# controlGroupUsersOnAddToCartStep.clear()


print('--------------------------------------\r\n\r\n')

#------------------
#------------------
#------------------


# 1 get unique users in test group uutg
print('--------------------------------------')
print('test group unique users', len(testGroupUniuqueUsers))

# 2 get intersection of uutg and users from next step: landing viewed

testGroupUsersOnLandingViewedStep = getNextFunnelStepResult('Landing Viewed.csv', testGroupUniuqueUsers, 'landingViewed')
testGroupUniuqueUsers.clear()
#------------------
# 3 get intersection of uutg and users from next step: address change initiated
testGroupUsersOnAddressChangeInitiatedStep = getNextFunnelStepResult('Address Change Initiated.csv', testGroupUsersOnLandingViewedStep, 'addressChangeInitiated')
testGroupUsersOnLandingViewedStep.clear()
#------------------

# 4 get intersection of uutg and users from next step: shop selection started

testGroupUsersOnShopSelectionStartedStep = getNextFunnelStepResult('Shop Selection Started.csv', testGroupUsersOnAddressChangeInitiatedStep, 'shopSelectionStarted')
testGroupUsersOnAddressChangeInitiatedStep.clear()
#------------------
# 5 get intersection of uutg and users from next step: shop selected

testGroupUsersOnShopSelectedStep = getNextFunnelStepResult('Shop Selected.csv', testGroupUsersOnShopSelectionStartedStep, 'shopSelected')
testGroupUsersOnShopSelectionStartedStep.clear()
#------------------
# get intersection of uutg and users from next step: main page viewed

# testGroupUsersOnMainPageViewedStep = getNextFunnelStepResult('Main Page Viewed.csv', testGroupUsersOnShopSelectedStep, 'mainPageViewed')
# testGroupUsersOnShopSelectedStep.clear()
#------------------
# get intersection of uutg and users from next step: add to cart

# testGroupUsersOnAddToCartStep = getNextFunnelStepResult('Add To Cart Clicked.csv', testGroupUsersOnMainPageViewedStep, 'addToCartClicked', False)
# testGroupUsersOnMainPageViewedStep.clear()
#------------------
# 6 get intersection of uutg and users from next step: product added
# instead of next

testGroupUsersOnProductAddedStep = getNextFunnelStepResult('Product Added.csv', testGroupUsersOnShopSelectedStep, 'productAdded', False)
# np.savetxt('/result/data.csv', (testGroupUsersOnProductAddedStep), delimiter=',')
saveToFile('test-group.json', testGroupUsersOnProductAddedStep)
testGroupUsersOnShopSelectedStep.clear()
testGroupUsersOnProductAddedStep.clear()

# testGroupUsersOnProductAddedStep = getNextFunnelStepResult('Product Added.csv', testGroupUsersOnAddToCartStep, 'productAdded', False)
# testGroupUsersOnAddToCartStep.clear()
#------------------