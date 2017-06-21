# MSGScrapeSubscriptionExample.py
import _thread as thread
import blpapi
import time
from blpapi import Event as EventType
from optparse import OptionParser


SESSION_STARTED = blpapi.Name("SessionStarted")
SESSION_STARTUP_FAILURE = blpapi.Name("SessionStartupFailure")
TOKEN_SUCCESS = blpapi.Name("TokenGenerationSuccess")
TOKEN_FAILURE = blpapi.Name("TokenGenerationFailure")
AUTHORIZATION_SUCCESS = blpapi.Name("AuthorizationSuccess")
AUTHORIZATION_FAILURE = blpapi.Name("AuthorizationFailure")
TOKEN = blpapi.Name("token")
AUTH_SERVICE = "//blp/apiauth"

EVENT_TYPE_NAMES = {
    EventType.ADMIN: "ADMIN",
    EventType.SESSION_STATUS: "SESSION_STATUS",
    EventType.SUBSCRIPTION_STATUS: "SUBSCRIPTION_STATUS",
    EventType.REQUEST_STATUS: "REQUEST_STATUS",
    EventType.RESPONSE: "RESPONSE",
    EventType.PARTIAL_RESPONSE: "PARTIAL_RESPONSE",
    EventType.SUBSCRIPTION_DATA: "SUBSCRIPTION_DATA",
    EventType.SERVICE_STATUS: "SERVICE_STATUS",
    EventType.TIMEOUT: "TIMEOUT",
    EventType.AUTHORIZATION_STATUS: "AUTHORIZATION_STATUS",
    EventType.RESOLUTION_STATUS: "RESOLUTION_STATUS",
    EventType.TOPIC_STATUS: "TOPIC_STATUS",
    EventType.TOKEN_STATUS: "TOKEN_STATUS",
    EventType.REQUEST: "REQUEST"
}


class Error(Exception):
    pass

def getAuthenticationOptions(authType, name, authDirSvc):
    if authType == "NONE":
        return None
    elif authType == "USER_APP":
        return "AuthenticationMode=USER_AND_APPLICATION;AuthenticationType=OS_LOGON;ApplicationAuthenticationType=APPNAME_AND_KEY;ApplicationName=" + name
    elif authType == "DIRSVC_APP":
        return "AuthenticationMode=USER_AND_APPLICATION;AuthenticationType=DIRECTORY_SERVICE;DirSvcPropertyName=" + authDirSvc + ";ApplicationAuthenticationType=APPNAME_AND_KEY;ApplicationName=" + name
    elif authType == "APPLICATION":
        return "AuthenticationMode=APPLICATION_ONLY;ApplicationAuthenticationType=APPNAME_AND_KEY;ApplicationName=" + name
    elif authType == "DIRSVC":
        return "AuthenticationType=DIRECTORY_SERVICE;DirSvcPropertyName=" + authDirSvc
    else:
        return "AuthenticationType=OS_LOGON"


def topicName(security):
    if security.startswith("//"):
        return security
    else:
        return "//blp/mktdata/" + security
        #refdata
        #mktdata
        #mktvwap


def printMessage(msg, eventType):
    print ("#{0} msg received: [{1}] => {2}/{3}".format(
        thread.get_ident(),
        ", ".join(map(str, msg.correlationIds())),
        EVENT_TYPE_NAMES[eventType],
        msg))


def parseCmdLine():
    parser = OptionParser()
    parser.add_option("-a",
                      "--host",
                      dest="host",
                      help="Host address to connect to. (default: %default)",
                      metavar="<host>",
                      default="bbg-gre-uat-mbpipe.lon.ib.commerzbank.com")
    parser.add_option("-p",
                      "--port",
                      dest="port",
                      type="int",
                      help="Port number to connect to. (default: %default)",
                      metavar="<port>",
                      default=8194)
    parser.add_option("-s",
                      "--security",
                      dest="securities",
                      help="Security to subscribe to. (default: MSGSCRP MSG1 Curncy)",
                      metavar="<security>",
                      action="append",
                      #default=["isin/US0394831020 "]
                      )
    parser.add_option("-o",
                      dest="options",
                      help="Subscription options, for example EID=45899",
                      metavar="<options>",
                      action="append",
                      #default="EID=27749"
                      )
    parser.add_option("",
                      "--auth-type",
                      type="choice",
                      choices=["LOGON", "NONE", "APPLICATION", "DIRSVC", "USER_APP", "DIRSVC_APP"],
                      dest="authType",
                      help="Authentication type: [LOGON (default) | NONE | APPLICATION | DIRSVC | USER_APP | DIRSVC_APP]",
                      metavar="<auth type>",
                      default="APPLICATION")
    parser.add_option("",
                      "--auth-name",
                      dest="authName",
                      help="The name of application or directory service",
                      metavar="<auth name>",
                      default="COMMERZBANK:MarketAlert")
    parser.add_option("",
                      "--auth-dirSvc",
                      dest="authDirSvc",
                      help="The name of directory service",
                      metavar="<dir svc name>",
                      default="mail")

    (options, args) = parser.parse_args()

    # options.maxEvents = 1000000
    #options.fields = ["LAST_PRICE"]

    options.auth = getAuthenticationOptions(options.authType, options.authName, options.authDirSvc)

    print(options)

    return options


# Subscribe 'session' for the securities and fields specified in 'options'
def subscribe(session, options, identity=None):
    sl = blpapi.SubscriptionList()
    for s in options.securities:
        topic = topicName(s)
        cid = blpapi.CorrelationId(s)
        print ("Subscribing {0} => {1}".format(cid, topic))
        sl.add(topic, options.fields, options.options, correlationId=cid)
    session.subscribe(sl, identity)

def unsubscribe(session, options, identity=None):
    sl = blpapi.SubscriptionList()
    for s in options.securities:
        cid = blpapi.CorrelationId(s)
        print(cid)
        print ("Unsubscribing {0} => {1}".format(cid, s))
        sl.add(s, correlationId=cid)
    session.unsubscribe(sl)

# Event handler
def processEvent(event, session):
    global identity
    global options

    try:
        eventType = event.eventType()
        for msg in event:
            # Print all incoming messages including  SubscriptionData
            #printMessage(msg, eventType)

            if eventType == EventType.SESSION_STATUS:
                if msg.messageType() == SESSION_STARTED:
                    # Session.startAsync completed successfully. Start authorization if needed
                    if options.auth:
                        # Generate token
                        session.generateToken()
                    else:
                        identity = None
                        # Subscribe for the specified securities/fields without identity
                        #subscribe(session, options)
                        pass
                elif msg.messageType() == SESSION_STARTUP_FAILURE:
                    # Session.startAsync failed, raise exception to exit
                    raise Error("Can't start session")

            elif eventType == EventType.TOKEN_STATUS:
                if msg.messageType() == TOKEN_SUCCESS:
                    # Token generated successfully. Continue the authorization
                    # Get generated token
                    token = msg.getElementAsString(TOKEN)
                    # Open auth service (we do it syncroniously, just in case)
                    if not session.openService(AUTH_SERVICE):
                        raise Error("Failed to open auth service")
                    # Obtain opened service
                    authService = session.getService(AUTH_SERVICE)
                    # Create and fill the authorization request
                    authRequest = authService.createAuthorizationRequest()
                    authRequest.set(TOKEN, token)
                    # Create Identity
                    identity = session.createIdentity()
                    # Send authorization request to "fill" the Identity
                    session.sendAuthorizationRequest(authRequest, identity)
                else:
                    # Token generation failed, raise exception to exit
                    raise Error("Failed to generate token")

            elif eventType == EventType.RESPONSE \
                    or eventType == EventType.PARTIAL_RESPONSE:
                if msg.messageType() == AUTHORIZATION_SUCCESS:
                    # Authorization passed, identity "filled" and can be used
                    # Subscribe to the specified securities/fields with identity
                    #subscribe(session, options, identity)

                    pass
                elif msg.messageType() == AUTHORIZATION_FAILURE:
                    # Authorization failed, raise exception to exit
                    raise Error("Failed to pass authorization")

            #elif eventType == EventType.SUBSCRIPTION_DATA:
                    #print(msg.correlationIds())
                    #print(msg)

    except Error as ex:
        print ("Error in event handler:", ex)
        # Interrupt a "sleep loop" in main thread
        thread.interrupt_main()

def main():
    global options
    options = parseCmdLine()

    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)
    if options.auth:
        sessionOptions.setAuthenticationOptions(options.auth)

    print("Connecting to %s:%s" % (options.host, options.port))
    # Create a Session
    session = blpapi.Session(sessionOptions)


    # Start a Session
    if not session.start():
        print("Failed to start session.")
        return

    try:
        identity = session.createIdentity()
        print(identity)
        #Open service to get historical data from
        if not session.openService("//blp/refdata"):
            print("Failed to open //blp/refdata")
            return

        # Obtain previously opened service
        refDataService = session.getService("//blp/refdata")

        # Create and fill the request for the historical data
        request = refDataService.createRequest("HistoricalDataRequest")
        request.getElement("securities").appendValue("IBM US Equity")
        request.getElement("fields").appendValue("PX_LAST")
        request.set("startDate", "20170101")
        request.set("endDate", "20061231")
        request.set("maxDataPoints", 100)

        print("Sending Request:", request)
        # Send the request
        session.sendRequest(request,identity)

        # Process received events
        while(True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = session.nextEvent(500)
            for msg in ev:
                print(msg)

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break
    finally:
        # Stop the session
        session.stop()

if __name__ == "__main__":
    print("SimpleHistoryExample")
    try:
        main()
    except KeyboardInterrupt:
        print("Ctrl+C pressed. Stopping...")


__copyright__ = """
Copyright 2016. Bloomberg Finance L.P.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:  The above
copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
"""
