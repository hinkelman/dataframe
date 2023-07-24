(library (dataframe bind)
  (export dataframe-append
          dataframe-bind)

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
  
  ;; bind -----------------------------------------------------------------------------------

  
  (define dataframe-bind
    (case-lambda
      [(dfs) (df-bind 'na dfs)]
      [(fill-value dfs) (df-bind fill-value dfs)]))
  
  (define (df-bind fill-value dfs)
    (check-all-dataframes dfs "(dataframe-bind fill-value dfs)")
    (let* ([names (combine-names-ordered dfs)]
           [ls-vals (map (lambda (name) (bind-rows name fill-value dfs)) names)])
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

  (define (bind-rows name fill-value dfs)
    (apply append
           (map (lambda (df)
                  (if (dataframe-contains? df name)
                      (dataframe-values df name)
                      (make-list (car (dataframe-dim df)) fill-value))) 
                dfs)))

  )

