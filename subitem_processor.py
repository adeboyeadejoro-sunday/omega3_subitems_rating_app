import pandas as pd

class SubitemProcessor:
    def __init__(
        self,
        df: pd.DataFrame,
        corrections: dict[str, str] | None = None,
        flat_csv_path: str = "flat_subitems.csv",
        counts_csv_path: str = "subitem_rating_counts.csv"
    ):
        """
        df: raw Monday.com export with subitems baked in.
        corrections: optional mapping of typo → canonical subitem names.
        flat_csv_path: where to write the flattened table.
        counts_csv_path: where to write the pivoted counts table.
        """
        self.df = df.reset_index(drop=True)
        self.corrections = corrections or {}
        self.flat_csv_path = flat_csv_path
        self.counts_csv_path = counts_csv_path
        
        # these get populated when you call process()
        self.flat_df: pd.DataFrame
        self.counts: pd.DataFrame

    def _flatten(self) -> pd.DataFrame:
        flat_rows = []
        i = 0
        n_rows = len(self.df)
        
        while i < n_rows:
            main = self.df.iloc[i]
            rating_cell = str(main['SubItem-Rating']).strip()
            if not rating_cell:
                i += 1
                continue

            sub_ratings = [r.strip() for r in rating_cell.split(',') if r.strip()]
            n_sub = len(sub_ratings)

            # attempt to grab the subitem-header row
            try:
                header = self.df.iloc[i + 1]
                hdr = header.astype(str).str.strip().str.lower()
                name_col   = hdr[hdr == 'name'].index[0]
                rating_col = hdr[hdr == 'item-rating'].index[0]
            except Exception:
                # skip this “main” if no proper header follows
                print(f"⚠️ Skipping row {i}: no valid subitem header found.")
                i += 1
                continue

            entries = self.df.iloc[i + 2 : i + 2 + n_sub]
            for entry in entries.itertuples(index=False, name=None):
                flat_rows.append({
                    'parent_unique_id': main['Unique Element-ID'],
                    'parent_SKU':       main['SKU'],
                    'parent_LOT':       main['LOT'],
                    'subitem_name':     entry[self.df.columns.get_loc(name_col)],
                    'subitem_rating':   entry[self.df.columns.get_loc(rating_col)],
                })

            i += 2 + n_sub

        flat = pd.DataFrame(flat_rows)
        if self.corrections:
            flat['subitem_name'] = flat['subitem_name'].replace(self.corrections)
        return flat

    def _pivot_counts(self, flat: pd.DataFrame) -> pd.DataFrame:
        counts = (
            flat
              .groupby(['subitem_name', 'subitem_rating'])
              .size()
              .unstack(fill_value=0)
              .sort_index()
        )
        return counts

    def process(self):
        """Run the full flatten → pivot pipeline and save CSVs."""
        # 1) flatten
        self.flat_df = self._flatten()
        self.flat_df.to_csv(self.flat_csv_path, index=False)
        print(f"Flattened table saved to {self.flat_csv_path}")

        # 2) pivot counts
        self.counts = self._pivot_counts(self.flat_df)
        self.counts.to_csv(self.counts_csv_path)
        print(f"Counts table saved to {self.counts_csv_path}")

        return self.flat_df, self.counts


if __name__ == "__main__":
    # Example usage:
    df = pd.read_csv("Q2 not ok overview - lab test results with subitems.csv", dtype='str').fillna('')  # your raw Monday export
    corrections = {
        'Active': 'Actives', 'Actves': 'Actives', 'Actvies': 'Actives',
        'Haevy metals': 'Heavy metals', 'Heavy metal': 'Heavy metals',
        'Mycrotoxins': 'Mycotoxins',
        'Solvent': 'Solvents',
        'Steriods': 'Steroids',
        'TOTOX': 'Totox',
        'Vitamines': 'Vitamins',
    }

    proc = SubitemProcessor(
        df=df,
        corrections=corrections,
        flat_csv_path="outputs/flat_q2.csv",
        counts_csv_path="outputs/counts_q2.csv"
    )
    flat_df, counts = proc.process()

    # If you also want percentages (rounded to 2 decimals):
    pct = (counts.div(counts.sum(axis=1), axis=0) * 100).round(2)
    pct.to_csv("outputs/percentages_q2.csv")
    print("Percentages saved to outputs/percentages_q2.csv")
