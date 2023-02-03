from pymongo import MongoClient
import json
import tweepy
from tweepy.streaming import Stream
# from tweepy import OAuthHandler
import datetime
import pytz


# Membuat koneksi dengan MongoDB lokal
connection = MongoClient('localhost', 27017, tz_aware=True)
# Memilih Database pada MongoDB, bernama TwitterStream
db = connection.TwitterStream
# Memilih Collection pada Database, bernama tweets
collection = db.tweetsStream


# Memasukkan Values yang tersedia pada Twitter Developer Portal untuk menghubungkan ke API Twitter
consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""

# Kode untuk mengambil/streaming tweet


class MyStreamListener(Stream):

    def on_data(self, data):

        # memuat tweet ke variabel t
        t = json.loads(data)

        # Menarik Informasi tweet untuk disimpan dalam database

        try:
            # Mengambil tweet pada field full_text tweet jika ada. (biasanya kalau >140 character akan disingkat lalu muncul field extended_tweet full_text)
            tweetStr = t['extended_tweet']['full_text']
        except Exception as e:
            tweetStr = t['text']    # Mengambil tweet dari field text
        tweet_id = t['id_str']      # ID Tweet dari Twitter dalam format string
        username = t['user']['screen_name']  # username dari tweet
        # text = t['text']          # The entire body of the Tweet
        text = tweetStr             # isi dari tweet
        dt = t['created_at']        # timestamp dari tweet
        language = t['lang']        # bahasa dari tweet

        # Mengubah timestamp string dari Twitter menjadi date object "created". sehingga dengan mudah dimanipulasi di MongoDB
        # dt = Thu Jun 30 11:33:18 +0000 2022
        # created = datetime.datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')
        created = datetime.datetime.strptime(dt, '%a %b %d %H:%M:%S %z %Y')
        # ubah ke timezone jakarta
        created_tz_jakarta = created.astimezone(pytz.timezone('Asia/Jakarta'))
        # isi created
        # 2022-06-30 12:01:35
        # ngedekor: indihome gimana sih lambat banget Thu Jun 30 12:01:35 +0000 2022
        # 2022-06-30 12:09:48+00:00
        # ngedekor: biznet gimana nih Thu Jun 30 12:09:48 +0000 2022
        print("-----------------------------------------")
        print("Waktu UTC: ")
        print(created)
        print("Waktu Jakarta: ")
        print(created_tz_jakarta)
        print("Bahasa: ")
        print(language)
        lokasi = 'bebas'
        # Memuat semua data yang telah diekstrak dari tweet ke dalam variabel "tweet" yang akan disimpan dalam database
        tweet = {'tweet_id': tweet_id, 'username': username,
                 'text': text, 'language': language, 'created': created_tz_jakarta, 'lokasi': lokasi}
        # Simpan data ke MongoDB
        # collection.insert_one(tweet)
         
        # Print/Menampilkan username, tweet, dan tanggal setiap tweet di console secara realtime ketika tweet diambil dari Twitter
        print(username + ':' + ' ' + text + ' ' + dt + '')
        print("******************************************")
        return True

    # Prints the reason for an error to your console
    # def on_error(self, status):
    #     print(status)
    # menangani error
    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False


# Tweepy code
#
if __name__ == '__main__':

    myStreamListener_new = MyStreamListener(
        consumer_key, consumer_secret, access_token, access_token_secret)
    # kata kunci yang akan dicari
    myStreamListener_new.filter(
        track=['FirstMedia,@FirstMediaCares,Biznet,@BiznetHome,Indihome,@Indihome'])
