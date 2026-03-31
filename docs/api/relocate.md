# Relocate

## `(dataframe-relocate df names [where anchor])`

**returns:** a dataframe where the columns in the `names` are moved, by default, to the front of all columns in `df`. Optionally, indicate `where` columns should be placed relative to the `anchor` column. Valid options for `where` are `'before` and `'after`.

```scheme
> (define df
    (make-df*
     (a 1 2 3)
     (b 4 5 6)
     (c 7 8 9)
     (d 10 11 12)))

> (dataframe-display df)

 dim: 3 rows x 4 cols
       a       b       c       d
   <num>   <num>   <num>   <num>
      1.      4.      7.     10.
      2.      5.      8.     11.
      3.      6.      9.     12.

> (dataframe-display
    (dataframe-relocate df '(c d)))

 dim: 3 rows x 4 cols
       c       d       a       b
   <num>   <num>   <num>   <num>
      7.     10.      1.      4.
      8.     11.      2.      5.
      9.     12.      3.      6.

> (dataframe-display
    (dataframe-relocate df '(a) 'after 'c))
    
 dim: 3 rows x 4 cols
       b       c       a       d
   <num>   <num>   <num>   <num>
      4.      7.      1.     10.
      5.      8.      2.     11.
      6.      9.      3.     12.

> (dataframe-display
    (dataframe-relocate df '(d c) 'before 'b))

 dim: 3 rows x 4 cols
       a       d       c       b
   <num>   <num>   <num>   <num>
      1.     10.      7.      4.
      2.     11.      8.      5.
      3.     12.      9.      6.
```
