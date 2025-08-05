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
        n_rows = len(self.df)
        i = 0

        while i < n_rows:
            row = self.df.iloc[i]
            unique_id = str(row.get('Unique Element-ID', '')).strip()

            # Check if this is a main row (Unique Element-ID is all digits)
            if unique_id.isdigit():
                parent_unique_id = unique_id
                parent_SKU = row.get('SKU', '')
                parent_LOT = row.get('LOT', '')

                # Check if next row is the Subitems header
                if i + 1 < n_rows:
                    header_row = self.df.iloc[i + 1]
                    if str(header_row[0]).strip().lower() == 'subitems':
                        # Find column indices for Name and Item-Rating
                        hdr = header_row.astype(str).str.strip().str.lower()
                        try:
                            name_col = hdr[hdr == 'name'].index[0]
                            rating_col = hdr[hdr == 'item-rating'].index[0]
                        except IndexError:
                            print(f"⚠️ Skipping main row {i}: Subitem headers not found properly.")
                            i += 2
                            continue

                        # Process subitem rows until next main row or EOF
                        j = i + 2
                        while j < n_rows:
                            next_row = self.df.iloc[j]
                            next_unique_id = str(next_row.get('Unique Element-ID', '')).strip()

                            if next_unique_id.isdigit():
                                # Next main row reached — stop subitem processing
                                break

                            # This is a subitem row
                            subitem_name = next_row[name_col]
                            subitem_rating = next_row[rating_col]

                            flat_rows.append({
                                'parent_unique_id': parent_unique_id,
                                'parent_SKU': parent_SKU,
                                'parent_LOT': parent_LOT,
                                'subitem_name': subitem_name,
                                'subitem_rating': subitem_rating,
                            })

                            j += 1

                        i = j  # Move i to next main row
                        continue
                    else:
                        print(f"⚠️ Skipping main row {i}: Subitems header not found.")
            i += 1

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
