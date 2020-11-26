(library (dataframe join)
  (export dataframe-left-join)

  (import (chezscheme)
          (dataframe bind)
          (dataframe split)
          (only (dataframe df)
                check-all-dataframes
                make-dataframe
                dataframe-names
                dataframe-contains?
                dataframe-alist
                dataframe-dim)   
          (only (dataframe helpers)
                not-in
                check-names-unique
                alist-drop
                alist-repeat-rows))

  (define (dataframe-left-join df1 df2 join-names missing-value)
    (let ([proc-string "(dataframe-left-join df1 df2 join-names missing-value)"])
      (check-all-dataframes (list df1 df2) proc-string)
      (check-join-names-exist df1 "df1" proc-string join-names)
      (check-join-names-exist df2 "df2" proc-string join-names)
      (let ([df1-not-join-names (not-in (dataframe-names df1) join-names)]
            [df2-not-join-names (not-in (dataframe-names df2) join-names)])
        (check-names-unique (append df1-not-join-names df2-not-join-names) proc-string)))
    (let-values ([(df1-ls df1-grp) (dataframe-split-helper df1 join-names #t)]
                 [(df2-ls df2-grp) (dataframe-split-helper df2 join-names #t)])
      (apply dataframe-bind
             (map (lambda (df grp)
                    (if (> (length (filter (lambda (x) (equal? x grp)) df2-grp)) 0)
                        (join-match df grp df2-ls df2-grp join-names missing-value)
                        (join-no-match df df2 join-names missing-value)))
                  df1-ls df1-grp))))

  (define (check-join-names-exist df df-name who names)
    (unless (apply dataframe-contains? df names)
      (assertion-violation who (string-append "not all join-names in " df-name))))
  
  ;; df1 and grp1 are elements of df1-ls and df1-grp
  (define (join-match df1 grp1 df2-ls df2-grp join-names missing-value)
    (let* ([df2 (cdr (assoc grp1 (map cons df2-grp df2-ls)))]
           [df1-rows (car (dataframe-dim df1))]
           [df2-rows (car (dataframe-dim df2))]
           [alist1 (dataframe-alist df1)]
           [alist2 (dataframe-alist df2)]
           [alist1-new (if (>= df1-rows df2-rows)
                           alist1
                           (alist-repeat-rows alist1 df2-rows 'times))]
           [alist2-new (if (>= df2-rows df1-rows)
                           alist2
                           (alist-repeat-rows alist2 df1-rows 'times))])
      (make-dataframe
       (append alist1-new (alist-drop alist2-new join-names)))))
  
  (define (join-no-match df df2 join-names missing-value)
    (make-dataframe
     (append (dataframe-alist df)
             (alist-drop 
              (alist-fill-missing (dataframe-names df2)
                                  (car (dataframe-dim df))
                                  missing-value)
              join-names))))

  (define (alist-fill-missing names n missing-value)
    (map (lambda (name) (cons name (make-list n missing-value))) names))

  )

