from fastai.structured import *
from fastai.column_data import *
np.set_printoptions(threshold=50, edgeitems=50)

PATH = "/Users/edwizzle/Developer/fastai_v2/data/rossmann/"

def concat_csvs(dirname):
    path = f'{PATH}{dirname}'
    filenames = glob(f"{path}/*.csv")

    wrote_header = False
    with open(f"{path}.csv", "w") as outputfile:
        for filename in filenames:
            name = filename.split(".")[0]
            with open(filename) as f:
                line = f.readline()
                if not wrote_header:
                    wrote_header = True
                    outputfile.write("file," + line)
                for line in f:
                    outputfile.write(name + "," + line)
                outputfile.write("\n")


# concat_csvs('googletrend')
# concat_csvs('weather')


table_names = ['train', 'store', 'store_states', 'state_names', 'googletrend', 'weather', 'test']

tables = [pd.read_csv(f'{PATH}{fname}.csv', low_memory=False) for fname in table_names]

from IPython.display import HTML

for t in tables: display(t.head())

for t in tables: display(DataFrameSummary(t).summary())

train, store, store_states, state_names, googletrend, weather, test = tables

len(train), len(test)

train.StateHoliday = train.StateHoliday!='0'
test.StateHoliday = test.StateHoliday!='0'

def join_df(left, right, left_on, right_on=None, suffix='_y'):
    if right_on is None: right_on = left_on
    return left.merge(right, how='left', left_on=left_on, right_on=right_on,
                      suffixes=("", suffix))

weather = join_df(weather, state_names, "file", "StateName")


googletrend['Date'] = googletrend.week.str.split(' - ', expand=True)[0]
googletrend['State'] = googletrend.file.str.split('_', expand=True)[2]
googletrend.loc[googletrend.State=='NI', "State"] = 'HB,NI'

add_datepart(weather, "Date", drop=False)
add_datepart(googletrend, "Date", drop=False)
add_datepart(train, "Date", drop=False)
add_datepart(test, "Date", drop=False)

trend_de = googletrend[googletrend.file == 'Rossmann_DE']

store = join_df(store, store_states, "Store")
len(store[store.State.isnull()])

joined = join_df(train, store, "Store")
joined_test = join_df(test, store, "Store")
len(joined[joined.StoreType.isnull()]), len(joined_test[joined_test.StoreType.isnull()])

joined = join_df(joined, googletrend, ["State", "Year", "Week"])
joined_test = join_df(joined_test, googletrend, ["State", "Year", "Week"])
len(joined[joined.trend.isnull()]), len(joined_test[joined_test.trend.isnull()])

joined = joined.merge(trend_de, 'left', ["Year", "Week"], suffixes=('', '_DE'))
joined_test = joined_test.merge(trend_de, 'left', ["Year", "Week"], suffixes=('', '_DE'))
len(joined[joined.trend_DE.isnull()]), len(joined_test[joined_test.trend_DE.isnull()])

