(library (dataframe reshape)
  (export dataframe-stack
          dataframe-spread)

  (import (chezscheme)
          (dataframe modify)
          (dataframe join)
          (dataframe split)
          (only (dataframe df)
                check-df-names
                dataframe-alist
                dataframe-contains?
                dataframe-dim
                dataframe-drop
                dataframe-names
                dataframe-rename
                dataframe-select
                dataframe-unique
                dataframe-values-map
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

  ;; this is not efficient but is relatively straightforward
  (define (dataframe-spread df names-from values-from missing-value)
    (let ([proc-string "(dataframe-spread df names-from values-from missing-value)"])
      (check-df-names df proc-string names-from values-from)
      (let* ([not-spread-names (not-in (dataframe-names df)
                                       (list names-from values-from))]
             [left-df (dataframe-unique
                       (apply dataframe-select df not-spread-names))])
        (let-values ([(df-ls df-grp) (dataframe-split-helper df (list names-from) #t)])
          (unless (for-all (lambda (grp) (or (symbol? (cadar grp)) (string? (cadar grp)))) df-grp)
            (assertion-violation proc-string "values in names-from must be strings or symbols"))
          (let ([df-ls-mod (map (lambda (df-local grp)
                                  (let* ([nf1 (cadar grp)]
                                         [nf2 (if (symbol? nf1) nf1 (string->symbol nf1))])
                                    (dataframe-drop
                                     (dataframe-rename df-local (list (list values-from nf2)))
                                     names-from)))
                                df-ls df-grp)])
            (dataframe-left-join-all (cons left-df df-ls-mod) not-spread-names missing-value))))))
  
  )

