# Read & Write

## Writing

### `(dataframe-write df path [overwrite])`

**writes:** dataframe `df` as a Scheme object to `path`. This format preserves all types. `overwrite` defaults to `#t`.

### `(dataframe->csv df path [overwrite])`

**writes:** dataframe `df` as a comma-separated file to `path`. `overwrite` defaults to `#t`.

### `(dataframe->tsv df path [overwrite])`

**writes:** dataframe `df` as a tab-separated file to `path`. `overwrite` defaults to `#t`.

---

## Reading

### `(dataframe-read path)`

**returns:** a dataframe read from a Scheme object file at `path`. Types are preserved.

### `(csv->dataframe path [header])`

**returns:** a dataframe read from a CSV file at `path`. `header` defaults to `#t`.

### `(tsv->dataframe path [header])`

**returns:** a dataframe read from a TSV file at `path`. `header` defaults to `#t`.

---

## Example

```scheme
> (define df
    (make-df*
      (Boolean #t #f #t)
      (Char #\y #\e #\s)
      (String "these" "are" "strings")
      (Symbol 'these 'are 'symbols)
      (Number 1.1 2 3.2)))

> (dataframe-write df "df-example.scm")

> (dataframe-display (dataframe-read "df-example.scm"))
 ;; types are preserved
 dim: 3 rows x 5 cols
  Boolean    Char   String   Symbol  Number
   <bool>   <chr>    <str>    <sym>   <num>
       #t       y    these    these  1.1000
       #f       e      are      are  2.0000
       #t       s  strings  symbols  3.2000

> (dataframe->csv df "df-example.csv")

> (dataframe-display (csv->dataframe "df-example.csv"))
 ;; types are not preserved when reading CSV
 dim: 3 rows x 5 cols
  Boolean    Char   String   Symbol  Number
    <str>   <str>    <str>    <str>   <num>
       #t       y    these    these  1.1000
       #f       e      are      are  2.0000
       #t       s  strings  symbols  3.2000
```

!!! note
    `dataframe-write` / `dataframe-read` round-trips preserve types. `dataframe->csv`/`dataframe-tsv` round-trips do not — all columns are read back as strings or numbers.
