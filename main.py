import pandas as pd
import re
from unidecode import unidecode


def clean_text(value):
    if pd.isna(value):
        return None
    text = str(value).lower()
    text = unidecode(text)
    text = re.sub(r'[^a-z0-9]', '', text)
    return text if text else None


def clean_domain(value):
    if pd.isna(value):
        return None
    text = str(value).lower().strip()
    text = unidecode(text)
    text = re.sub(r'^https?://', '', text)
    text = re.sub(r'^www\.', '', text)
    text = text.split('/')[0]
    text = re.sub(r':\d+$', '', text)
    text = re.sub(r'[^a-z0-9\.]', '', text)
    return text if text else None


def process_data(df):
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
    df['phone'] = df['phone'].apply(clean_text)
    df['domain'] = df['domain'].apply(clean_domain)

    df['id'] = 0
    next_id = 1

    print("incep")
    for i in range(len(df)):
        if df.loc[i, 'id'] != 0:
            continue

        df.loc[i, 'id'] = next_id

        d1 = df.loc[i, 'domain']
        lat1 = df.loc[i, 'lat']
        lon1 = df.loc[i, 'lon']
        p1 = df.loc[i, 'phone']

        for j in range(i + 1, len(df)):
            if df.loc[j, 'id'] != 0:
                continue

            same = False

            d2 = df.loc[j, 'domain']
            if pd.notna(d1) and pd.notna(d2) and d1 == d2:
                same = True

            if not same:
                lat2 = df.loc[j, 'lat']
                lon2 = df.loc[j, 'lon']
                if pd.notna(lat1) and pd.notna(lon1) and pd.notna(lat2) and pd.notna(lon2):
                    if lat1 == lat2 and lon1 == lon2:
                        same = True

            if not same:
                p2 = df.loc[j, 'phone']
                if pd.notna(p1) and pd.notna(p2) and p1 == p2:
                    same = True

            if same:
                df.loc[j, 'id'] = next_id

        next_id += 1

    print("a mers")

    df = df.sort_values(by='id').reset_index(drop=True)
    return df


def main():
    data = pd.read_excel("veridion_entity_resolution_challenge.xlsx")

    data = data.rename(columns={
        data.columns[11]: 'lat',
        data.columns[12]: 'lon',
        data.columns[32]: 'phone',
        data.columns[38]: 'domain'
    })

    print("incep test 1000")
    sample = data.head(1000).copy()
    sample_result = process_data(sample)
    sample_result.to_csv("test.csv", index=False)
    print("a mers test 1000")

    print("incep full")
    full_result = process_data(data)
    full_result.to_csv("raspuns.csv", index=False)
    print("a mers full")


if __name__ == "__main__":
    main()
