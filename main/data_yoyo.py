"""This module features functions and classes to manipulate data for the
collaborative filtering algorithm.
"""

from pathlib import Path

import scipy
import pandas as pd
import numpy as np


def transform_data():
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

    print(f"Dataframe for 'language':")
    file_path = f"{data_fold}meta_song.parquet"
    df = pd.read_parquet(file_path, engine='pyarrow')
    song_id_language = list(np.array(df['song_id']))
    language_id = list(np.array(df['language_id']))
    # print(df['language_id'])

    print("\n")

    genre = [["userID", "artistID",	"weight"]]
    for i in range(100):
        if (i % 10 == 0):
            print(i)
        while True:
            try:
                temp = [session_id[i], song_id[i]]
                find_index = song_id_genre.index(song_id[i])

                temp.append(genre_id[find_index])

                find_lang_index = song_id_language.index(song_id[i])

                temp.append(language_id[find_lang_index])
                if (listening_order[i] == 1):
                    temp.append(2000)
                else:
                    temp.append(500)
                genre.append(temp)
                song_id_genre.pop(find_index)
                genre_id.pop(find_index)
                song_id_language.pop(find_lang_index)
                language_id.pop(find_lang_index)
            except ValueError:
                break

    print("\nthe corresponding genre")
    print(np.array(genre))
    for i in genre:
        print(i)


# def save_to_csv(name, data_output):
#     file_path = "./spider/rank_data/"
#     filename = file_path + f"{name}.csv"
#     np.savetxt(filename, data_output, encoding='utf-8',
#                delimiter=", ", fmt='%s')


def load_user_artists(user_artists_file: Path) -> scipy.sparse.csr_matrix:
    """Load the user artists file and return a user-artists matrix in csr
    fromat.
    """
    user_artists = pd.read_csv(user_artists_file, sep="\t")
    user_artists.set_index(["userID", "artistID"], inplace=True)
    coo = scipy.sparse.coo_matrix(
        (
            user_artists.weight.astype(float),
            (
                user_artists.index.get_level_values(0),
                user_artists.index.get_level_values(1),
            ),
        )
    )
    return coo.tocsr()


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
    transform_data()


if __name__ == "__main__":
    # user_artists_matrix = load_user_artists(
    #     Path("../lastfmdata/user_artists.dat")
    # )
    # print(user_artists_matrix)

    # main()
    debug()
