import pandas as pd
data_fold = 'datagame-2023/'
label_test_source = 'label_test_source.parquet'
label_train_source = 'label_train_source.parquet'
label_train_target = 'label_train_target.parquet'
data_files ={
    'test': 'label_test_source.parquet',
    'train_s': 'label_train_source.parquet',
    'train_t': 'label_train_target.parquet',
    'composer': 'meta_song_composer.parquet',
    'genre': 'meta_song_genre.parquet',
    'lyricist': 'meta_song_lyricist.parquet',
    'producer': 'meta_song_producer.parquet',
    'titletext': 'meta_song_titletext.parquet',
    'song_id': 'meta_song.parquet'
}
pd.read_parquet("datagame-2023/label_test_source.parquet", engine='pyarrow')

print("h")