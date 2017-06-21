# RefDataExampleBpipe.py
# - derived from RefDataTableOverrideExample.py, ServiceSchema.py
#   and SimpleAsyncSubscription.py
# Note that this has been lightly tested...
import blpapi
from optparse import OptionParser
from blpapi   import Event as EventType
from blpapi   import Identity

SECURITY_DATA           = blpapi.Name( "securityData"           )
SECURITY                = blpapi.Name( "security"               )
FIELD_DATA              = blpapi.Name( "fieldData"              )
FIELD_EXCEPTIONS        = blpapi.Name( "fieldExceptions"        )
FIELD_ID                = blpapi.Name( "fieldId"                )
ERROR_INFO              = blpapi.Name( "errorInfo"              )

SESSION_STARTED         = blpapi.Name( "SessionStarted"         )
SESSION_STARTUP_FAILURE = blpapi.Name( "SessionStartupFailure"  )
TOKEN_SUCCESS           = blpapi.Name( "TokenGenerationSuccess" )
TOKEN_FAILURE           = blpapi.Name( "TokenGenerationFailure" )
AUTHORIZATION_SUCCESS   = blpapi.Name( "AuthorizationSuccess"   )
AUTHORIZATION_FAILURE   = blpapi.Name( "AuthorizationFailure"   )
REFERENCE_DATA_RESPONSE = blpapi.Name( "ReferenceDataResponse"  )
TOKEN                   = blpapi.Name( "token"                  )

AUTH_SERVICE            = "//blp/apiauth"

EVENT_TYPE_NAMES = {
    EventType.ADMIN                : "ADMIN"                ,
    EventType.SESSION_STATUS       : "SESSION_STATUS"       ,
    EventType.SUBSCRIPTION_STATUS  : "SUBSCRIPTION_STATUS"  ,
    EventType.REQUEST_STATUS       : "REQUEST_STATUS"       ,
    EventType.RESPONSE             : "RESPONSE"             ,
    EventType.PARTIAL_RESPONSE     : "PARTIAL_RESPONSE"     ,
    EventType.SUBSCRIPTION_DATA    : "SUBSCRIPTION_DATA"    ,
    EventType.SERVICE_STATUS       : "SERVICE_STATUS"       ,
    EventType.TIMEOUT              : "TIMEOUT"              ,
    EventType.AUTHORIZATION_STATUS : "AUTHORIZATION_STATUS" ,
    EventType.RESOLUTION_STATUS    : "RESOLUTION_STATUS"    ,
    EventType.TOPIC_STATUS         : "TOPIC_STATUS"         ,
    EventType.TOKEN_STATUS         : "TOKEN_STATUS"         ,
    EventType.REQUEST              : "REQUEST"
}

def parseCmdLine():
    parser = OptionParser(description="Retrieve reference data from BPipe.")
    parser.add_option("-a",
                      "--host",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default="localhost")
    parser.add_option("-p",
                      "--port",
                      dest="port",
                      type="int",
                      help="PORT to connect to (%default)",
                      metavar="PORT",
                      default=8194)
    parser.add_option("-s",
                      "--security",
                      dest="securities",
                      help="security to subscribe to "
                      "(-s 'IBM US Equity' by default)",
                      metavar="SECURITY",
                      action="append",
                      default=[])
    parser.add_option("-f",
                      "--fields",
                      dest="fields",
                      help="FIELDS to subscribe to "
                      "(-f 'LAST_PRICE' -f 'BID' -f 'ASK' by default)",
                      metavar="FIELDS",
                      action="append",
                      default=[])
    parser.add_option("",
                      "--auth-type",
                      type="choice",
                      choices=["LOGON", "NONE", "APPLICATION", "DIRSVC",
                      "USER_APP"],
                      dest="authType",
                      help="Authentication type: LOGON (default), NONE, "
                      "APPLICATION, DIRSVC or USER_APP",
                      default="LOGON")
    parser.add_option("",
                      "--auth-name",
                      dest="authName",
                      help="The name of application or directory service",
                      default="")
    parser.add_option("",
                      "--auth-dirSvc",
                      dest="authDirSvc",
                      help="The name of directory service",
                      metavar="<dir svc name>",
                      default="mail")

    (options, args) = parser.parse_args()

    if not options.securities:
        options.securities = ["IBM US Equity"]

    if not options.fields:
        options.fields = ["PX_LAST","PX_BID","PX_ASK"]

    options.auth = getAuthenticationOptions(options.authType,
                                              options.authName)

    print("auth options passed:", options.authType, options.authName)
    return options

def getAuthenticationOptions(auth_type, name):
    if auth_type == "NONE":
        return None
    elif auth_type == "USER_APP":
        return "AuthenticationMode=USER_AND_APPLICATION;"\
            "AuthenticationType=OS_LOGON;"\
            "ApplicationAuthenticationType=APPNAME_AND_KEY;"\
            "ApplicationName=" + name
    elif auth_type == "APPLICATION":
        return "AuthenticationMode=APPLICATION_ONLY;"\
            "ApplicationAuthenticationType=APPNAME_AND_KEY;"\
            "ApplicationName=" + name
    elif auth_type == "DIRSVC":
        return "AuthenticationType=DIRECTORY_SERVICE;"\
            "DirSvcPropertyName=" + name
    else:
        return "AuthenticationType=OS_LOGON"

def printMessage(msg):
    if msg.messageType() != REFERENCE_DATA_RESPONSE:
        print("[{0}]: {1}".format(", ".join(map(str, msg.correlationIds())),
                                  msg))
    else:
        # This case demonstrates how to get values of individual elements
        securityDataArray = msg.getElement("securityData")
        for securityData in securityDataArray.values():
            securityName = securityData.getElementValue("security")
            print(securityName)
            fieldData = securityData.getElement("fieldData")
            for fieldName in options.field:
                try:
                    fieldValue = fieldData.getElementValue(fieldName)
                    print("%s %s" % (fieldName, fieldValue))
                except:
                    print("%s n/a" % fieldName)

def auth(session):
    eq = blpapi.EventQueue()

    # Generate token
    session.generateToken(eventQueue=eq)

    # Process related response
    ev = eq.nextEvent()
    token = None
    if ev.eventType() == blpapi.Event.TOKEN_STATUS:
        for msg in ev:
            printMessage(msg)
            if msg.messageType() == TOKEN_SUCCESS:
                token = msg.getElementAsString(TOKEN)
            elif msg.messageType() == TOKEN_FAILURE:
                break
    if not token:
        raise Exception("Failed to get token")

    # Purge EventQueue to reuse one for the next request
    eq.purge()

    # Open authentication service
    if not session.openService(AUTH_SERVICE):
        raise Exception("Failed to open auth service")

    # Obtain opened service
    authService = session.getService(AUTH_SERVICE)

    # Create and fill the authorization request
    authRequest = authService.createAuthorizationRequest()
    authRequest.set(TOKEN, token)

    # Create Identity
    identity = session.createIdentity()

    # Send authorization request to "fill" the Identity
    session.sendAuthorizationRequest(authRequest, identity, eventQueue=eq)

    # Process related responses
    while True:
        ev = eq.nextEvent()
        if ev.eventType() in set([
                blpapi.Event.RESPONSE,
                blpapi.Event.PARTIAL_RESPONSE,
                blpapi.Event.REQUEST_STATUS]):
            for msg in ev:
                printMessage(msg)
                if msg.messageType() == AUTHORIZATION_SUCCESS:
                    # auth passed, identity "filled"
                    return identity
                else:
                    raise Exception("Authorization failed")


def main():
    global options
    options = parseCmdLine()

    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)

    if options.auth:
        sessionOptions.setAuthenticationOptions(options.auth)

    print("Connecting to %s:%d" % (options.host, options.port))

    # Create a Session
    session = blpapi.Session(sessionOptions)

    # Start a Session
    if not session.start():
        print("Failed to start session.")
        return

    print("Session started.")

    # Perform authentication
    if options.auth:
        print("auth options used: ",options.auth)
        identity = auth(session)

        print("Authentication passed: ",)
        if(identity.getSeatType()==Identity.BPS):
            print("BPS User")
        elif(identity.getSeatType()==Identity.NONBPS):
            print("Non-BPS User")
        else:
            print("Invalid User")
            return 9
    else:
        identity = None
        print("No authentication specified")

    # Open service to get reference data from
    if not session.openService("//blp/refdata"):
        print("Failed to open //blp/refdata")
        return

    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("HistoricalDataRequest")

    # Add securities to request.
    for s in options.securities:
        request.append("securities", s)

    # Add fields to request.
    for f in options.fields:
        request.append("fields", f)

    # Create and fill the request for the historical data
    request.set("periodicityAdjustment", "ACTUAL")
    request.set("periodicitySelection", "MONTHLY")
    request.set("startDate", "20170501")
    request.set("endDate", "20170531")
    request.set("maxDataPoints", 100)

    if identity == None:
        print("Sending Request without identity:", request)
        cid = session.sendRequest(request)
    else:
        print("Sending Request with identity:", request)
        cid = session.sendRequest(request,identity)

    try:
        # Process received events
        while(True):
            # We provide timeout to give the chance to Ctrl+C handling:
            ev = session.nextEvent(500)
            for msg in ev:
                if cid in msg.correlationIds():
                    # Process the response generically.
                    print(msg)
            # Response completely received, so we could exit
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
    finally:
        # Stop the session
        session.stop()

if __name__ == "__main__":
    print("RefDataExampleBpipe")
    try:
        main()
    except KeyboardInterrupt:
        print("Ctrl+C pressed. Stopping...")

__copyright__ = """
Copyright 2012. Bloomberg Finance L.P.

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
