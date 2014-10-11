import com.gmongo.GMongo
import groovy.transform.Field

@Grapes( [
        @Grab( group = 'org.codehaus.groovy.modules.http-builder', module = 'http-builder', version = '0.7.1' ),
        @Grab( group = 'com.gmongo', module = 'gmongo', version = '1.3' )
] )

import groovyx.net.http.RESTClient

@Field count = 0
@Field afterId = ""
@Field dataset = [ ]
@Field api = new RESTClient()
@Field mongo = new GMongo( "localhost" );
@Field db = mongo.getDB( "test-lang-data" )

def subreddits = [ "haskell" ]
subreddits.each {
    getSubredditData( it, "" )
}

def getSubredditData( subreddit, query ) {
    count++

    def response = api.get( uri: "https://www.reddit.com/r/$subreddit/new/.json$query" )
    response.data.data.children.each { story ->
        def commentsResponse = api.get( uri: "http://www.reddit.com${ story.data.permalink }.json" )
        def listing = commentsResponse.data[ 0 ].data.children[ 0 ].data
        def comments = [ ]
        def addComments
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



        addComments = { children ->

            children.each { child ->
                def obj = child.data
                comments.add( [
                        body            : obj.body,
                        created         : obj.created ? new Date( obj.created.toLong() * 1000 ) : null,
                        created_utc     : obj.created_utc ? new Date( obj.created_utc.toLong() * 1000 ) : null,
                        author          : obj.author,
                        name            : obj.name,
                        parentId        : obj.parent_id,
                        controversiality: obj.controversiality,
                        ups             : obj.ups,
                        downs           : obj.downs
                ] )

                if ( child.data.replies && child.data.replies.data.children )
                    addComments( child.data.replies.data.children )
            }
        }

        def firstLevelComments = commentsResponse.data[ 1 ].data.children
        if ( firstLevelComments )
            addComments( firstLevelComments )

        def entry = [ story: topic, comments: comments ]
        db.subreddits.insert( entry )
        dataset.add( entry )

//        println( "Title: ${ entry.story.title }, Comment Count: ${ entry.comments.size() }" )
//        println( "\tFirst Comment: ${ entry.comments[ 0 ]?.body }" )
    }

    println( "Count: ${ dataset.size() }, Page: $count" )
    afterId = response.data.data.after
    if ( afterId && count < 5 )
        getSubredditData( subreddit, "?after=$afterId&limit=100&count=${ dataset.size() }" )


}

println( "Dataset Size: ${ dataset.size() }" )