(library (dataframe bind)
  (export dataframe-append
          dataframe-bind
          dataframe-bind-all)

  (import (rnrs)
          (only (dataframe df)
                check-all-dataframes
                dataframe-alist
                dataframe-contains?
                dataframe-dim
                dataframe-names
                dataframe-values
                make-dataframe)   
          (only (dataframe helpers)
                make-list
                combine-names-ordered
                get-all-names
                get-all-unique-names
                check-names-unique))

  ;; append -----------------------------------------------------------------------------------

  (define (dataframe-append . dfs)
    (let ([proc-string "(dataframe-append dfs)"]
          [all-names (apply get-all-names (map dataframe-alist dfs))])
      (check-all-dataframes dfs proc-string)
      (check-names-unique all-names proc-string)
      (unless (apply = (map (lambda (df) (car (dataframe-dim df))) dfs))
        (assertion-violation proc-string "all dfs must have same number of rows")))
    (make-dataframe (apply append (map (lambda (df) (dataframe-alist df)) dfs))))
  
  ;; bind -----------------------------------------------------------------------------------

  (define (dataframe-bind . dfs)
    (let ([proc-string "(dataframe-bind dfs)"])
      (check-all-dataframes dfs proc-string)
      (if (= (length dfs) 1)
          (car dfs)
          (let ([names (apply shared-names dfs)])
            (when (null? names) (assertion-violation proc-string "no names in common across dfs"))
            (let ([alist (map (lambda (name)
                                ;; missing-value will not be used so chose arbitrary value (-999)
                                (cons name (apply bind-rows name -999 dfs)))
                              names)])
              (make-dataframe alist))))))
  
  (define (shared-names . dfs)
    (let ([first-names (dataframe-names (car dfs))]
          [rest-names (apply get-all-unique-names (map dataframe-alist (cdr dfs)))])
      (filter (lambda (name) (member name rest-names)) first-names)))

  (define (dataframe-bind-all missing-value . dfs)
    (check-all-dataframes dfs "(dataframe-bind-all missing-value dfs)")
    (let* ([names (apply combine-names-ordered (map dataframe-alist dfs))]
           [alist (map (lambda (name)
                         (cons name (apply bind-rows name missing-value dfs)))
                       names)])
      (make-dataframe alist)))

  (define (bind-rows name missing-value . dfs)
    (apply append
           (map (lambda (df)
                  (if (dataframe-contains? df name)
                      (dataframe-values df name)
                      (make-list (car (dataframe-dim df)) missing-value))) 
                dfs)))

  )

