(library (dataframe record-types)
  (export
   dataframe-slist
   dataframe-names
   dataframe-dim
   make-dataframe
   make-series
   series-name
   series-lst
   series-length)

  (import (rnrs)
          (datatable-types))


  ;; series -------------------------------------------------------------------

  (define-record-type series
    ;; series-src is not exported
    (fields name src lst type length)
    (protocol
     (lambda (new)
       (lambda (name src)
         (let* ([type (guess-type src 1000)]
                [lst (convert-type src type)])
           (new name src lst type (length lst)))))))
  
  ;; dataframe ----------------------------------------------------------------

  (define-record-type dataframe
    (fields slist names dim)
    (protocol
     (lambda (new)
       (lambda (slist)
         (let* ([names (map series-name slist)]
                [rows (series-length (car slist))]
                [cols (length names)])
           (new slist names (cons rows cols)))))))
  
  )

