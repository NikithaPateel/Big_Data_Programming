import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import time


consumer_key="RiAljDExhBmC58Iqh0fyv3Kia"
consumer_secret="4Bo2UzGErfBGJARHVBA0zkrA3pLCxgj916MwDJzwbdp3PwVUFn"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section

access_token="391206417-Ws9xmwz1zfJBGD6dlj7RNej8EroI3e26kRpwov5d"
access_token_secret="LDbm2pGU7IMQ0QiACTmnokPLePxfSW01eR5K8R64YNTQy"



auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['deep learning'])


if __name__ == "__main__":
    s = socket.socket()  # Create a socket object
    host = "localhost"  # Get local machine name
    port = 1234  # Reserve a port for your service.
    s.bind((host, port))  # Bind to the port
    print("Listening on port: %s" % str(port))
    s.listen(5)  # Now wait for client connection.
    c, addr = s.accept()  # Establish connection with client.
    print("Received request from: " + str(addr))
    time.sleep(5)
    sendData(c)