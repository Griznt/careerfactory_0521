import pandas as pd
from csv import DictReader
import datetime


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
def getNextFunnelStepResult(filename, previousStepData, _timestampName):
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
                        if not newData.get(id):
                            newData[id] = []
                        if not session.get(timestampName):
                            session[timestampName] = []
                        # if timedelta < datetime.timedelta(hours=1):
                        session[timestampName].append(currentEventDate)
                        newData[id].append(session)
    print('users count on step ' + _timestampName, len(newData))
    return newData

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

for userId, sessions in uniqueUsers.items():

    controlGroupSessionTimestamps = []
    testGroupSessionTimestamps = []
    for session in sessions:
        if session.get('group') == 'default':
            controlGroupSessionTimestamps.append(session.get('hit_at'))
        if session.get('group') == 'address_first':
            testGroupSessionTimestamps.append(session.get('hit_at'))

    if len(controlGroupSessionTimestamps) > 0 and len(testGroupSessionTimestamps) > 0:
        mixedGroup.append(userId)
        # del uniqueUsers[userId]
    elif len(controlGroupSessionTimestamps) > 0:
        controlGroupUniqueUsers[userId] = controlGroupSessionTimestamps
    elif len(testGroupSessionTimestamps) > 0:
        testGroupUniuqueUsers[userId] = testGroupSessionTimestamps

print('ready.',
'\r\nuniqueUsersLength:', len(uniqueUsers),
'\r\ncontrol group len:', len(controlGroupUniqueUsers),
'\r\ntest group len:', len(testGroupUniuqueUsers),
'\r\nmixed users:', len(mixedGroup))

# get unique users substract mixed users
print('unique users substact users in both groups:', len(uniqueUsers) - len(mixedGroup))
uniqueUsers.clear()

# get intersection of uucg and users from next step: landing viewed
controlGroupUsersOnLandingViewedStep = {}
with open('ext/Landing Viewed.csv', 'r') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        id = row['anonymous_id']
        userSessions = controlGroupUniqueUsers.get(id)
        if userSessions:
            # checking current intersection with users sessions:
            currentEventDate = row['timestamp']
            for sessionTime in userSessions:
                timedelta = abs( parseDate(currentEventDate) - parseDate(sessionTime) )
                if timedelta < datetime.timedelta(hours=ONE_SESSION_LENGTH):
                    if not controlGroupUsersOnLandingViewedStep.get(id):
                        controlGroupUsersOnLandingViewedStep[id] = []
                    controlGroupUsersOnLandingViewedStep[id].append({'hit_at': sessionTime, 'landingViewedTimestamp': currentEventDate})

print('usersOnLandingViewedStep len', len(controlGroupUsersOnLandingViewedStep))
# {'id': '05cb2fdc-ec9d-4e1e-b2eb-d9db3926c324', 'hit_at': ['2020-11-26 12:54:18.386 UTC', '2020-11-27 08:42:49.059 UTC'], 'landingViewedTimestamp': '2020-11-27 07:46:45.84 UTC'}
# 2020-11-26 12:54:18.448 UTC,05cb2fdc-ec9d-4e1e-b2eb-d9db3926c324
# 2020-11-27 07:46:45.84 UTC,05cb2fdc-ec9d-4e1e-b2eb-d9db3926c324            



#------------------
# get intersection of uucg and users from next step: main page viewed

# controlGroupUsersOnMainPageViewedStep = getNextFunnelStepResult('Main Page Viewed.csv', controlGroupUsersOnLandingViewedStep, 'mainPageViewed')
# controlGroupUsersOnLandingViewedStep.clear()
#------------------
# get intersection of uucg and users from next step: address change initiated

# controlGroupUsersOnAddressChangeInitiatedStep = getNextFunnelStepResult('Address Change Initiated.csv', controlGroupUsersOnMainPageViewedStep, 'addressChangeInitiated')
# controlGroupUsersOnMainPageViewedStep.clear()
#------------------
# get intersection of uucg and users from next step: main page viewed

# controlGroupUsersOnMainPageViewed2Step = getNextFunnelStepResult('Main Page Viewed.csv', controlGroupUsersOnAddressChangeInitiatedStep, 'mainPageViewed2')
# controlGroupUsersOnAddressChangeInitiatedStep.clear()
#------------------
# get intersection of uucg and users from next step: add to cart

# controlGroupUsersOnAddToCartStep = getNextFunnelStepResult('Add To Cart Clicked.csv', controlGroupUsersOnMainPageViewed2Step, 'addToCartClicked')
# controlGroupUsersOnMainPageViewed2Step.clear()
#------------------
# get intersection of uucg and users from next step: product added

# controlGroupUsersOnProductAddedStep = getNextFunnelStepResult('Product Added.csv', controlGroupUsersOnAddToCartStep, 'productAdded')
# controlGroupUsersOnAddToCartStep.clear()
#------------------
#------------------
#------------------


# get unique users in test group uutg

testGroupUniuqueUsers

# get intersection of uutg and users from next step: landing viewed

testGroupUsersOnLandingViewedStep = {}
with open('ext/Landing Viewed.csv', 'r') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        id = row['anonymous_id']
        userSessions = testGroupUniuqueUsers.get(id)
        if userSessions:
            # checking current intersection with users sessions:
            currentEventDate = row['timestamp']
            for sessionTime in userSessions:
                timedelta = abs( parseDate(currentEventDate) - parseDate(sessionTime) )
                if timedelta < datetime.timedelta(hours=ONE_SESSION_LENGTH):
                    if not testGroupUsersOnLandingViewedStep.get(id):
                        testGroupUsersOnLandingViewedStep[id] = []
                    testGroupUsersOnLandingViewedStep[id].append({'hit_at': sessionTime, 'landingViewedTimestamp': currentEventDate})

print('testGroupUsersOnLandingViewed', len(testGroupUsersOnLandingViewedStep))

#------------------
# get intersection of uutg and users from next step: address change initiated
testGroupUsersOnAddressChangeInitiatedStep = getNextFunnelStepResult('Address Change Initiated.csv', testGroupUsersOnLandingViewedStep, 'addressChangeInitiated')
testGroupUsersOnLandingViewedStep.clear()
#------------------

# get intersection of uutg and users from next step: shop selection started

testGroupUsersOnShopSelectionStartedStep = getNextFunnelStepResult('Shop Selection Started.csv', testGroupUsersOnAddressChangeInitiatedStep, 'shopSelectionStarted')
testGroupUsersOnAddressChangeInitiatedStep.clear()
#------------------
# get intersection of uutg and users from next step: shop selected

testGroupUsersOnShopSelectedStep = getNextFunnelStepResult('Shop Selected.csv', testGroupUsersOnShopSelectionStartedStep, 'shopSelected')
testGroupUsersOnShopSelectionStartedStep.clear()
#------------------
# get intersection of uutg and users from next step: main page viewed

testGroupUsersOnMainPageViewedStep = getNextFunnelStepResult('Main Page Viewed.csv', testGroupUsersOnShopSelectedStep, 'mainPageViewed')
testGroupUsersOnShopSelectedStep.clear()
#------------------
# get intersection of uutg and users from next step: add to cart

testGroupUsersOnAddToCartStep = getNextFunnelStepResult('Add To Cart Clicked.csv', testGroupUsersOnMainPageViewedStep, 'addToCartClicked')
testGroupUsersOnMainPageViewedStep.clear()
#------------------
# get intersection of uutg and users from next step: product added

testGroupUsersOnProductAddedStep = getNextFunnelStepResult('Product Added.csv', testGroupUsersOnMainPageViewedStep, 'productAdded')
testGroupUsersOnMainPageViewedStep.clear()
#------------------
