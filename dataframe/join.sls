(library (dataframe join)
  (export dataframe-inner-join
          dataframe-left-join
          dataframe-left-join-all
          dataframe-full-join)

  (import (rnrs)
          (dataframe bind)
          (dataframe split)
          (dataframe rename)
          (dataframe record-types)
          (only (dataframe filter)
                slist-ref)
          (only (dataframe select)
                slist-drop)
          (only (dataframe assertions)
                check-names-unique)
          (only (dataframe helpers)
                make-list
                not-in))

  (define dataframe-left-join-all
    (case-lambda
      [(dfs)
       (dataframe-left-join-all dfs #f 'na)]
      [(dfs join-names)
       (dataframe-left-join-all dfs join-names 'na)]
      [(dfs join-names fill-value)
       (let loop ([dfs (cdr dfs)]
                  [out (car dfs)])
         (if (null? dfs)
             out
             (loop (cdr dfs)
                   (dataframe-left-join
                    out (car dfs) join-names fill-value))))]))

  (define dataframe-left-join
    (case-lambda
      [(df1 df2)
       (dataframe-left-join df1 df2 #f 'na)]
      [(df1 df2 join-names)
       (dataframe-left-join df1 df2 join-names 'na)]
      [(df1 df2 join-names fill-value)
       (let* ([who "(dataframe-left-join df1 df2 join-names fill-value)"]
              [names-pair (parse-join-names df1 df2 join-names)]
              [names1 (car names-pair)]
              [names2 (cadr names-pair)]
              [df2-aligned (align-df2 df2 names2 names1 who)])
         (check-join df1 df2-aligned names1 who)
         (let ([slists1 (dataframe-split-helper df1 names1 who)]
               [slists2 (dataframe-split-helper df2-aligned names1 who)])
           (dataframe-bind-all
            (df-left-join-helper slists1 slists2 names1 fill-value))))]))

  (define dataframe-inner-join
    (case-lambda
      [(df1 df2)
       (dataframe-inner-join df1 df2 #f)]
      [(df1 df2 join-names)
       (let* ([who "(dataframe-inner-join df1 df2 join-names)"]
              [names-pair (parse-join-names df1 df2 join-names)]
              [names1 (car names-pair)]
              [names2 (cadr names-pair)]
              [df2-aligned (align-df2 df2 names2 names1 who)])
         (check-join df1 df2-aligned names1 who)
         (let ([slists1 (dataframe-split-helper df1 names1 who)]
               [slists2 (dataframe-split-helper df2-aligned names1 who)])
           (dataframe-bind-all
            (df-inner-join-helper slists1 slists2 names1))))]))

  (define dataframe-full-join
    (case-lambda
      [(df1 df2)
       (dataframe-full-join df1 df2 #f 'na)]
      [(df1 df2 join-names)
       (dataframe-full-join df1 df2 join-names 'na)]
      [(df1 df2 join-names fill-value)
       (let* ([who "(dataframe-full-join df1 df2 join-names fill-value)"]
              [names-pair (parse-join-names df1 df2 join-names)]
              [names1 (car names-pair)]
              [names2 (cadr names-pair)]
              [df2-aligned (align-df2 df2 names2 names1 who)])
         (check-join df1 df2-aligned names1 who)
         (let ([slists1 (dataframe-split-helper df1 names1 who)]
               [slists2 (dataframe-split-helper df2-aligned names1 who)])
           (dataframe-bind-all
            (df-full-join-helper slists1 slists2 names1 fill-value))))]))

  (define (check-join df1 df2 join-names who)
    (check-all-dataframes (list df1 df2) who)
    (check-join-names-exist df1 "df1" who join-names)
    (check-join-names-exist df2 "df2" who join-names)
    (let ([df1-not-join-names
           (not-in (dataframe-names df1) join-names)]
          [df2-not-join-names
           (not-in (dataframe-names df2) join-names)])
      (check-names-unique (append df1-not-join-names df2-not-join-names) who)))

  (define (check-join-names-exist df df-name who names)
    (unless (apply dataframe-contains? df names)
      (assertion-violation who (string-append "not all join-names in " df-name))))

  ;; Parse join-names
  ;; Input for lst can be:
  ;;   - #f or omitted: auto-detect common names
  ;;   - '(a b c): list of symbols, same names in both dfs
  ;;   - '((a . x) (b . y)): list of pairs, different names in each df
  (define (parse-join-names df1 df2 lst)
    (cond
     [(not lst)
      (let ([common (filter (lambda (n) (dataframe-contains? df2 n))
                            (dataframe-names df1))])
        (when (null? common)
          (assertion-violation
           "(dataframe-join)"
           "no common column names found; specify join-names explicitly"))
        (list common common))]
     [(and (pair? lst) (pair? (car lst)))
      (list (map car lst) (map cdr lst))]
     [else
      (list lst lst)]))

  ;; When df2's join column names differ from df1's, rename them in df2
  ;; so the rest of the join machinery works unchanged
  (define (align-df2 df2 names2 names1 who)
    (if (equal? names1 names2)
        df2
        (begin
          ;; names2 must exist in df2
          (unless (apply dataframe-contains? df2 names2)
            (assertion-violation who "not all join-names found in df2"))
          ;; renaming must not collide with existing non-join columns in df2
          (let ([non-join-names (not-in (dataframe-names df2) names2)])
            (when (exists (lambda (n) (member n non-join-names)) names1)
              (assertion-violation who "join-names from df1 collide with existing columns in df2")))
          (dataframe-rename df2 names2 names1))))

  (define (df-left-join-helper slists1 slists2 join-names fill-value)
    (let* ([grps2 (map (lambda (x) (get-join-group x join-names)) slists2)]
           [grps2-slists2 (map cons grps2 slists2)])
      (map (lambda (slist)
             (let* ([grp (get-join-group slist join-names)]
                    [grp-match (assoc grp grps2-slists2)])
               (if grp-match
                   (join-match slist (cdr grp-match) join-names)
                   (join-no-match slist (car slists2) join-names fill-value))))
           slists1)))

  (define (df-inner-join-helper slists1 slists2 join-names)
    (let* ([grps2 (map (lambda (x) (get-join-group x join-names)) slists2)]
           [grps2-slists2 (map cons grps2 slists2)])
      (let loop ([slists slists1]
                 [out '()])
        (cond [(null? slists)
               (reverse out)]
              [(assoc (get-join-group (car slists) join-names) grps2-slists2)
               =>
               (lambda (grp-match)
                 (loop (cdr slists)
                       (cons
                        (join-match
                         (car slists)
                         (cdr grp-match) join-names)
                        out)))]
              [else
               (loop (cdr slists) out)]))))

  ;; Full join = all matched rows + df1-only rows (filled) + df2-only rows (filled).
  ;; We collect matched df2 groups as we go, then emit the unmatched df2 groups at the end.
  (define (df-full-join-helper slists1 slists2 join-names fill-value)
    (let* ([grps2 (map (lambda (x) (get-join-group x join-names)) slists2)]
           [grps2-slists2 (map cons grps2 slists2)])
      (let loop ([slists slists1]
                 [matched-grps '()]
                 [out '()])
        (if (null? slists)
            (let* ([unmatched-slists2
                    (map cdr
                         (filter (lambda (g+s) (not (member (car g+s) matched-grps)))
                                 grps2-slists2))]
                   [unmatched-rows
                    (map (lambda (slist2)
                           (join-no-match slist2 (car slists1) join-names fill-value))
                         unmatched-slists2)])
              (append (reverse out) unmatched-rows))   ;; <-- just return the list
            (let* ([slist (car slists)]
                   [grp (get-join-group slist join-names)]
                   [grp-match (assoc grp grps2-slists2)])
              (if grp-match
                  (loop (cdr slists)
                        (cons grp matched-grps)
                        (cons (join-match slist (cdr grp-match) join-names) out))
                  (loop (cdr slists)
                        matched-grps
                        (cons (join-no-match slist (car slists2) join-names fill-value)
                              out))))))))

  ;; Takes an slist and returns the values from the first row of the join columns.
  ;; All values in join-names columns are the same because the slist was split on them.
  (define (get-join-group slist join-names)
    (apply append (map series-lst (slist-ref slist '(0) join-names))))

  ;; Drop join-names columns from slist2 to avoid duplication, then append.
  ;; Repeat rows for whichever slist has fewer.
  (define (join-match slist1 slist2 join-names)
    (let* ([n1 (series-length (car slist1))]
           [n2 (series-length (car slist2))]
           [slist2-drop (slist-drop slist2 join-names)]
           [slist1-new (if (>= n1 n2)
                           slist1
                           (slist-repeat-rows slist1 n2 'times))]
           [slist2-new (if (>= n2 n1)
                           slist2-drop
                           (slist-repeat-rows slist2-drop n1 'times))])
      (make-dataframe (append slist1-new slist2-new))))

  ;; Rows in slist1 have no match in slist2.
  ;; Retain non-join columns from slist2 but fill them with fill-value.
  (define (join-no-match slist1 slist2 join-names fill-value)
    (let* ([n (series-length (car slist1))]
           [slist2-names (map series-name slist2)]
           [slist2-names-sel (not-in slist2-names join-names)])
      (make-dataframe
       (append slist1 (slist-fill-missing slist2-names-sel n fill-value)))))

  (define (slist-fill-missing names n fill-value)
    (map (lambda (name) (make-series name (make-list n fill-value))) names))

  )
