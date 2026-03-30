# Scheme (R6RS) Dataframe Library

A dataframe record type with procedures to select, drop, and rename columns, and filter, sort, split, bind, append, join, reshape, and aggregate dataframes. [[Related blog posts](https://www.travishinkelman.com/#category=dataframe)]

## Documentation

Full API documentation is available at **[hinkelman.github.io/dataframe](https://hinkelman.github.io/dataframe)**.

## Installation

### Akku

```
$ akku install dataframe
```

For more information on getting started with [Akku](https://akkuscm.org/), see this [blog post](https://www.travishinkelman.com/posts/getting-started-with-akku-package-manager-for-scheme/).

## Import

```scheme
(import (dataframe))
```

## Quick Example

```scheme
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
