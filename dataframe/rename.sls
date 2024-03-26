(library (dataframe rename)
  (export
   dataframe-rename*
   dataframe-rename
   dataframe-rename-all)

  (import (rnrs)
          (dataframe record-types)
          (only (dataframe assertions)
                check-names))

  (define-syntax dataframe-rename* 
    (syntax-rules ()
      [(_ df (old-name new-name) ...)
       (let ([old-names (list (quote old-name) ...)]
             [new-names (list (quote new-name) ...)])
         (when (apply dataframe-contains? df new-names)
           (assertion-violation "(dataframe-rename* df (old-name new-name) ...)"
                                "new names duplicate existing names"))
         (dataframe-rename df old-names new-names))]))

  (define (dataframe-rename-all df new-names)
    (let ([old-names (dataframe-names df)]
          [names-length (length new-names)]
          [num-cols (cdr (dataframe-dim df))])
      (unless (= (length old-names) names-length num-cols)
        (assertion-violation
         "(dataframe-rename-all df new-names)"
         (string-append  "names length must be " (number->string num-cols)
                         ", not " (number->string names-length))))
      (dataframe-rename df old-names new-names)))
       
  (define (dataframe-rename df old-names new-names)
    (let ([who "(dataframe-rename df old-names new-names)"])
      (check-dataframe df who)
      (check-names new-names who))
    (make-dataframe
     (map (lambda (series)
            (let* ([name-pairs (map cons old-names new-names)]
                   [name (series-name series)]
                   [lst (series-lst series)]
                   [name-match (assoc name name-pairs)])
              (if name-match
                  (make-series (cdr name-match) lst)
                  series)))
          (dataframe-slist df))))

  )

