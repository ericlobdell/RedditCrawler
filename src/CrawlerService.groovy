import com.gmongo.GMongo
import groovy.time.TimeCategory
import groovy.transform.Field
import groovyx.net.http.HttpResponseException
@Grapes( [
        @Grab( group = 'org.codehaus.groovy.modules.http-builder', module = 'http-builder', version = '0.7.1' ),
        @Grab( group = 'com.gmongo', module = 'gmongo', version = '1.3' )
] )

import groovyx.net.http.RESTClient
import static groovyx.net.http.ContentType.URLENC
import net.sf.json.JSONObject

import static groovy.json.JsonOutput.prettyPrint

@Field int pageCount = 0
@Field api = new RESTClient()
@Field mongo = new GMongo( "localhost" )
@Field db = mongo.getDB( "test-lang-data" )

afterId = ""
datasetCount = 0
accessToken = ""
refreshToken = ""
authTime = ""
authExpires = 0

config = new ConfigSlurper().parse( new File("src/config.groovy").toURL(  ) )

def doPreRequestWork( scope ) {
    if (tokenExpired(  ))
        getAuthToken( true )

    headers = [ 'User-Agent': 'lobdellio_crawler/0.1 by lobdellio', "Authorization": "bearer $accessToken" ]
    sleep( 2000 )
}

def tokenExpired () {
    def elapsed = TimeCategory.minus(new Date(), authTime)
    return elapsed.getSeconds() > authExpires
}

def printResponse( res ) {
    def json = new JSONObject( res.getData() )
    println "Start Writing file..."
    new File( "test.json" ).withWriter { out ->
        out.write( prettyPrint( json.toString( 4 ) ) )
    }
    println "Finished Writing file."
}
def getAuthToken ( isRefresh ) {
    def authResponse
    def appId = config.reddit.appId
    def appSecret = config.reddit.appSecret
    def postHeaders = [
            "Authorization": "Basic ${ "$appId:$appSecret".bytes.encodeBase64().toString() }",
            "Content-Type": "application/json"]

    //def grantType = isRefresh ? "refresh_token" : "password"
    def postUrl = "https://ssl.reddit.com/api/v1/access_token?grant_type=password&username=$config.reddit.username&password=$config.reddit.password&duration=permanent"

//    if (isRefresh)
//        postUrl += "&refresh_token=$refreshToken"

    authResponse = api.post( uri: postUrl, headers: postHeaders, requestContentType: URLENC)

    printResponse( authResponse )

    authTime = new Date()
    accessToken = authResponse.data.access_token
  //  refreshToken = authResponse.refresh_token
    authExpires = authResponse.data.expires_in

    println( "Token: $accessToken, Auth Time: $authTime, Expires In: $authExpires" )
}

getAuthToken( false )

def subreddits = [ "haskell" ]
subreddits.each {
    getSubredditData it, "?limit=100"
}

def getSubredditData( subreddit, query ) {
    pageCount++

    doPreRequestWork( "outer" )
    try {
        def response = api.get(
                uri: "https://www.reddit.com/r/$subreddit/new/.json$query",
                headers: headers )


        response.data.data.children.each { story ->
            doPreRequestWork( "inner" )
            def commentsResponse = api.get(
                    uri: "http://www.reddit.com${ story.data.permalink }.json",
                    headers: headers )

            def listing = commentsResponse.data[ 0 ].data.children[ 0 ].data
            def comments = [ ]
            def addComments
            def meta = [
                    recorded: new Date()
            ]
            def topic = [
                    created      : listing.created ? new Date( listing.created.toLong() * 1000 ) : null,
                    created_utc  : listing.created_utc ? new Date( listing.created_utc.toLong() * 1000 ) : null,
                    upvote_ratio : listing.upvote_ratio.toDouble(),
                    url          : listing.url,
                    subreddit    : listing.subreddit,
                    likes        : listing.likes,
                    topic_id     : listing.id,
                    author       : listing.author,
                    score        : listing.score,
                    over_18      : listing.over_18,
                    subreddit_id : listing.subreddit_id,
                    ups          : listing.ups,
                    downs        : listing.downs,
                    name         : listing.name,
                    permalink    : listing.permalink,
                    stickied     : listing.stickied,
                    title        : listing.title,
                    num_comments : listing.num_comments,
                    distinguished: listing.distinguished
            ]

            addComments = { comms ->

                comms.data.each { d ->
                    comments.add( [
                            body            : d.body,
                            created         : d.created ? new Date( d.created.toLong() * 1000 ) : null,
                            created_utc     : d.created_utc ? new Date( d.created_utc.toLong() * 1000 ) : null,
                            author          : d.author,
                            name            : d.name,
                            parentId        : d.parent_id,
                            controversiality: d.controversiality,
                            ups             : d.ups,
                            downs           : d.downs
                    ] )

                    if ( d.replies && d.replies.data.children )
                        addComments d.replies.data.children
                }
            }

            def firstLevelComments = commentsResponse.data[ 1 ].data.children
            if ( firstLevelComments )
                addComments firstLevelComments

            def entry = [ story: topic, comments: comments, meta: meta ]
            datasetCount++
            db.subreddits.insert entry
        }


        if ( datasetCount > 1000 )
            println "WOOHOO! We id it!"

        afterId = response.data.data.after
        println "Count: $datasetCount, Page: $pageCount, AfterID: $afterId"
        if ( afterId && pageCount < 15 )
            getSubredditData subreddit, "?after=$afterId&limit=100"
    } catch ( HttpResponseException ex ) {
        def error = ex;
    }


}

println "Dataset Size: $datasetCount"