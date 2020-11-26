(library (dataframe reshape)
  (export dataframe-stack)

  (import (chezscheme)
          (dataframe modify)
          (only (dataframe df)
                check-df-names
                dataframe-alist
                dataframe-contains?
                dataframe-dim
                dataframe-names
                dataframe-values-map
                dataframe-select
                make-dataframe)
          (only (dataframe helpers)
                not-in
                rep
                alist-repeat-rows))

  (define (dataframe-stack df names names-to values-to)
    (let ([proc-string "(dataframe-stack df names-to values-to names)"])
      (apply check-df-names df proc-string names)
      (unless (symbol? names-to)
        (assertion-violation proc-string "names-to must be symbol"))
      (unless (symbol? values-to)
        (assertion-violation proc-string "values-to must be symbol"))
      (when (dataframe-contains? df names-to values-to)
        (assertion-violation proc-string "names-to or values-to already exist in df")))
    (let* ([other-names (not-in (dataframe-names df) names)]
           [alist-rep (alist-repeat-rows
                       (dataframe-alist (apply dataframe-select df other-names))
                       (length names)
                       'times)]
           [nrows (car (dataframe-dim df))]
           [names-to-vals (rep names nrows 'each)]
           [values-to-vals (apply append (dataframe-values-map df names))])
      (make-dataframe
       (append alist-rep
               (list (cons names-to names-to-vals))
               (list (cons values-to values-to-vals))))))
 
  )

