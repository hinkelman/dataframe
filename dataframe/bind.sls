(library (dataframe bind)
  (export dataframe-append
          dataframe-bind
          dataframe-bind-all)

  (import (rnrs)
          (dataframe record-types)
          (only (dataframe select)
                dataframe-values)
          (only (dataframe assertions)
                check-names-unique)
          (only (dataframe helpers)
                make-list))

  ;; append -----------------------------------------------------------------------------------

  (define (dataframe-append . dfs)
    (let ([who "(dataframe-append dfs)"]
          [all-names (get-all-names dfs)])
      (check-all-dataframes dfs who)
      (check-names-unique all-names who)
      (unless (apply = (map (lambda (df) (car (dataframe-dim df))) dfs))
        (assertion-violation who "all dfs must have same number of rows"))
      (make-dataframe
       (make-slist
        all-names
        (apply append (map (lambda (df) (map series-lst (dataframe-slist df))) dfs))))))

  (define (get-all-names dfs)
    (apply append (map (lambda (df) (dataframe-names df)) dfs)))
  
  ;; bind ------------------------------------------------------------------------------

  (define dataframe-bind
    (case-lambda
      [(df1 df2) (df-bind (list df1 df2) 'na)]
      [(df1 df2 fill-value) (df-bind (list df1 df2) fill-value)]))
  
  (define dataframe-bind-all
    (case-lambda
      [(dfs) (df-bind dfs 'na)]
      [(dfs fill-value) (df-bind dfs fill-value)]))
  
  (define (df-bind dfs fill-value)
    (check-all-dataframes dfs "(dataframe-bind dfs fill-value)")
    (let* ([names (combine-names-ordered dfs)]
           [ls-vals (map (lambda (name) (bind-rows dfs fill-value name)) names)])
    (make-dataframe (make-slist names ls-vals))))

  ;; combine names such that they stay in the order that they appear in each dataframe
  (define (combine-names-ordered dfs)
    (let loop ([all-names (get-all-names dfs)]
               [results '()])
      (cond [(null? all-names)
             (reverse results)]
            [(member (car all-names) results)
             (loop (cdr all-names) results)]
            [else
             (loop (cdr all-names) (cons (car all-names) results))])))

  (define (bind-rows dfs fill-value name)
    (apply append
           (map (lambda (df)
                  (if (dataframe-contains? df name)
                      (dataframe-values df name)
                      (make-list (car (dataframe-dim df)) fill-value))) 
                dfs)))

  )

