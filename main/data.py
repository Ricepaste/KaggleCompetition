"""This module features functions and classes to manipulate data for the
collaborative filtering algorithm.
"""

from pathlib import Path

import scipy
import pandas as pd
import numpy as np


def genre(coding_table):
    data_fold = 'datagame-2023/'

    # data_files = {
    #     'train_s': 'label_train_source.parquet',
    #     # 'genre': 'meta_song_genre.parquet',
    #     # 'song_id': 'meta_song.parquet'
    # }

    file_path = f"{data_fold}label_train_source.parquet"
    df = pd.read_parquet(file_path, engine='pyarrow')

    print(f"Dataframe for 'train_s':")
    song_id = list(np.array(df['song_id']))
    session_id = list(np.array(df['session_id']))
    listening_order = list(np.array(df['listening_order']))
    # print(df)
    # print(session_id)
    print("\n")

    file_path = f"{data_fold}meta_song_genre.parquet"
    df = pd.read_parquet(file_path, engine='pyarrow')

    print(f"Dataframe for 'genre':")
    song_id_genre = list(np.array(df['song_id']))
    genre_id = list(np.array(df['genre_id']))
    # print(df['song_id'])
    # print(df['genre_id'])
    print("\n")

    genre = [["userID", "artistID",	"weight"]]
    for i in range(100):
        if (i % 10 == 0):
            print(i)
        while True:
            try:
                temp = [session_id[i]]
                find_index = song_id_genre.index(song_id[i])

                temp.append(coding_table[genre_id[find_index]])
                if (listening_order[i] == 1):
                    temp.append(2000)
                else:
                    temp.append(500)
                genre.append(temp)
                song_id_genre.pop(find_index)
                genre_id.pop(find_index)
            except ValueError:
                break

    print("\nthe corresponding genre")
    print(np.array(genre))
    for i in genre:
        print(i)

    # for i in range(1, len(genre)):
    #     temp = list(np.frombuffer(
    #         genre[i][1].encode('utf-8'), dtype=np.uint8))
    #     temp = list(map(str, temp))
    #     genre[i][1] = int("".join(temp))
    save_to_csv("genre", np.array(genre))
    data_name()


def data_name():
    genre_dict = {"ce4db56f6a48426643b08038139a8a75": "國語歌曲Mandarin",
                  "6ea61b86b8fff0e3a05bc73ea4eaf21f": "西洋歌曲Western",
                  "1f4b914a79eb2bb01c2ea694af626625": "台語歌曲Hokkien",
                  "43244ec4c30a0dad837d982892bc0c05": "日語歌曲Japanese",
                  "bb3d7b04b67d5aeb5ab145bdd70750da": "粵語歌曲Cantonese",
                  "5f2a134d2289a8a3de6663ee8e248c8a": "原聲帶Soundtrack",
                  "bb737ea7450c3abbab1ff613a2c6309e": "嘻哈饒舌Hip-Hop/Rap",
                  "d9ab2ffb929a854e208efaf5297b7cf8": "爵士音樂Jazz",
                  "1d7c8bb87dcc1457ed90240c06f9ebdf": "電子/舞曲Electronic/Dance",
                  "7ed5eec2ea6f0208d27f78ef120e52fa": "演奏音樂Instrumental",
                  "157479e86322e5063bce2488bec94d88": "古典音樂Classical",
                  "ed514a72b48a9d15df7bc4c25eac2c67": "世界音樂World",
                  "65aeeca3341ca1c6a2ed774aa4e22add": "童謠歌曲Children",
                  "74694a488312db91fcd56818fed8b3a6": "懷念老歌Oldies",
                  "a3b51d979053a7f49511b9ab72ee8878": "其他音樂Other",
                  "5d7d428b81ab3429f189bbc642548b53": "電玩卡漫Games/Comic/Anime",
                  "1619db5683958ba19927468990d5ba44": "沙發音樂Lounge",
                  "d36204de09c0c6084d55b1f484a23773": "心靈音樂Spiritual",
                  "ee1696df08c0ac6005e3b9442cdbbded": "宗教音樂Religious",
                  "abdc0a50aff67c591737bc6f57a36e09": "節奏藍調/靈魂樂R&B/Soul",
                  "1a619fcf3adfd91711699b7e2cf2c367": "說唱藝術SpokenWord",
                  "2eabe9f164346c7b3ff1bd23078f483e": "搖滾/另類Rock/Alternative",
                  "e1d5802bb4e0f6ab79b2b9f4a5ff924b": "韓語歌曲Korean",
                  "03c358e326d99a3863e044c5d8e9fb50": "藍調歌曲Blues",
                  "4c4ce52d6f6a0e495c407b584ef3e020": "馬來/印尼Malay/Indo",
                  "80117354556efbf237f0020bea2f7e42": "印度語Tamil/Hindi",
                  "56203098bef705a44fee44ac0d9b7ef2": "泰文Thai",
                  "09994f2b6bf22721e7379df4e22b1041": "鄉村音樂Country",
                  "d48233dd673e7ba524a5a0e3389d39b9": "雷鬼音樂Reggae",
                  "a2a4a0943fe2c5fe6891d9c34dca906f": "另類Alternative",
                  "c1d8909eff8a140e7bd0c11508f76dbe": "民謠Folk",
                  "b856b6781d370a3645c6dde0c20b3597": "流行樂Pop",
                  "398e2d0befb1c6979e77e5ff7c3fcf07": "節慶音樂Holiday",
                  "b5e874310bf96c3fe29c54a01b982ad7": "演歌Enka",
                  "23ae47469cc0a10de43c453ab59c87b4": "基督教/福音Christian/Gospel",
                  "c3ad773a264597f9f46c2d666e1a8b50": "有聲書Audiobook",
                  "5455e1699025b025a3523aba4719e818": "有聲書/兒童青少Audiobook-Children's&Teenage",
                  "f6f02545c1905a8c72c5bf006579996e": "有聲書/教育Audiobook-Education"}
    ge = [["id", "name"]]
    coding_table = {}
    cur = 0
    for key, value in genre_dict.items():
        # temp = np.frombuffer(key.encode("utf-8"), dtype=np.uint8)
        # temp = list(map(str, temp))
        # key = int("".join(temp))
        # ge.append([cur, value])
        ge.append([cur, value])
        coding_table[key] = cur

        cur += 1

    # for i in range(1, len(genre)):
    #     temp = list(np.frombuffer(
    #         genre[i][1].encode('utf-8'), dtype=np.uint8))
    #     temp = list(map(str, temp))
    #     genre[i][1] = "".join(temp)
    save_to_csv("genre_name", np.array(ge))
    return coding_table


def save_to_csv(name, data_output):
    file_path = "./datagame-2023/"
    filename = file_path + f"{name}.dat"
    np.savetxt(filename, data_output, encoding='utf-8',
               delimiter="\t", fmt='%s')


def load_user_artists(user_artists_file: Path) -> scipy.sparse.csr_matrix:
    """Load the user artists file and return a user-artists matrix in csr
    fromat.
    """
    user_artists = pd.read_csv(user_artists_file, sep="\t", dtype='int64')
    # print(user_artists.artistID.astype('int64'))
    # user_artists = user_artists.values.tolist()
    # print(user_artists)
    # for i in range(2):
    #     print(type(user_artists[0][i]))
    #     print(type(user_artists[1][i]))
    user_artists.set_index(["userID", "artistID"], inplace=True)
    # turn user_artists to list
    coo = scipy.sparse.coo_matrix(
        (
            user_artists.weight.astype(float),
            (
                user_artists.index.get_level_values(0),
                user_artists.index.get_level_values(1),
            ),
        ),
    )
    return coo.tocsr()

# def turn_to_ASCII(arr):
#     for i in range(len(arr)):
#         arr[i] = arr[i].encode('ascii', 'ignore').decode('ascii')
#     return arr


class ArtistRetriever:
    """The ArtistRetriever class gets the artist name from the artist ID."""

    def __init__(self):
        self._artists_df = None

    def get_artist_name_from_id(self, artist_id: int) -> str:
        """Return the artist name from the artist ID."""
        return self._artists_df.loc[artist_id, "name"]

    def load_artists(self, artists_file: Path) -> None:
        """Load the artists file and stores it as a Pandas dataframe in a
        private attribute.
        """
        artists_df = pd.read_csv(artists_file, sep="\t")
        artists_df = artists_df.set_index("id")
        self._artists_df = artists_df


def main():
    artist_retriever = ArtistRetriever()
    artist_retriever.load_artists(Path("../lastfmdata/artists.dat"))
    artist = artist_retriever.get_artist_name_from_id(1)
    print(artist)


def debug():
    coding_table = data_name()
    genre(coding_table)


if __name__ == "__main__":
    # user_artists_matrix = load_user_artists(
    #     Path("../lastfmdata/user_artists.dat")
    # )
    # print(user_artists_matrix)

    # main()
    debug()
