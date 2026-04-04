# Scheme (R6RS) Dataframe Library

A dataframe record type with procedures to select, drop, and rename columns, and filter, sort, split, bind, append, join, reshape, and aggregate dataframes.

[Related blog posts](https://www.travishinkelman.com/#category=dataframe) · [Akku](https://akkuscm.org/packages/dataframe/)

---

## Quick Example

```scheme
(import (dataframe))

(define df
  (make-df*
    (grp 'a 'a 'b 'b 'b)
    (trt 'a 'b 'a 'b 'b)
    (adult 1 2 3 4 5)
    (juv 10 20 30 40 50)))

(dataframe-display
  (dataframe-filter* df (adult) (> adult 3)))
```

```
 dim: 2 rows x 4 cols
     grp     trt   adult     juv
   <sym>   <sym>   <num>   <num>
       b       b      4.     40.
       b       b      5.     50.
```

## API Overview

| Section | Procedures |
|---|---|
| [Types](api/types.md) | `get-type`, `guess-type`, `convert-type` |
| [Series](api/series.md) | `make-series`, `make-series*`, `series?`, `series-equal?`, …  |
| [Dataframe](api/dataframe.md) | `make-dataframe`, `make-df*`, `dataframe?`, `dataframe-equal?`, …  |
| [Display](api/display.md) | `dataframe-display`, `dataframe-glimpse` |
| [Read & Write](api/io.md) | `dataframe-read`, `csv->dataframe`, `dataframe-write`,  … |
| [Select & Drop](api/select.md) | `dataframe-select`, `dataframe-drop`, …  |
| [Rename](api/rename.md) | `dataframe-rename`, `dataframe-rename*`, `dataframe-rename-all` |
| [Relocate](api/relocate.md) | `dataframe-relocate` |
| [Filter](api/filter.md) | `dataframe-filter`, `dataframe-partition`, `dataframe-unique`,  … |
| [Sort](api/sort.md) | `dataframe-sort`, `dataframe-sort*` |
| [Split, Bind, & Append](api/split-bind-append.md) | `dataframe-split`, `dataframe-bind`, `dataframe-append`, … |
| [Crossing](api/crossing.md) | `dataframe-crossing` |
| [Join](api/join.md) | `dataframe-inner-join`, `dataframe-full-join`, `dataframe-left-join`, …  |
| [Reshape](api/reshape.md) | `dataframe-stack`, `dataframe-spread` |
| [Modify & Aggregate](api/modify-aggregate.md) | `dataframe-modify`, `dataframe-aggregate`, …  |
| [Thread Macros](api/thread.md) | `->`, `->>` |
| [Missing Values](api/missing-values.md) | `na?`, `any-na?`, `remove-na`, `dataframe-remove-na` |
| [Statistics](api/statistics.md) | `sum`, `mean`, `variance`, `quantile`, … |
