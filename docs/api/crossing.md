# Crossing

## `(dataframe-crossing obj1 obj2 ...)`

**returns:** a dataframe formed from the cartesian product of `obj1`, `obj2`, etc. Each argument must be either a series or a dataframe.

```scheme
> (dataframe-display
    (dataframe-crossing
      (make-series* (col1 'a 'b))
      (make-series* (col2 'c 'd))))

 dim: 4 rows x 2 cols
    col1    col2
   <sym>   <sym>
       a       c
       a       d
       b       c
       b       d

;; series and dataframe can be mixed
> (dataframe-display
    (dataframe-crossing
      (make-series* (col1 'a 'b))
      (make-df* (col2 'c 'd))))

 dim: 4 rows x 2 cols
    col1    col2
   <sym>   <sym>
       a       c
       a       d
       b       c
       b       d

;; multi-column dataframes produce row-wise combinations
> (dataframe-display
    (dataframe-crossing
      (make-df* (col1 'a 'b) (col2 'c 'd))
      (make-df* (col3 'e 'f) (col4 'g 'h))))

 dim: 4 rows x 4 cols
    col1    col2    col3    col4
   <sym>   <sym>   <sym>   <sym>
       a       c       e       g
       a       c       f       h
       b       d       e       g
       b       d       f       h
```
