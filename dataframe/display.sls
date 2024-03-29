
(library (dataframe display)
  (export dataframe-display
          dataframe-glimpse)

  (import (rnrs)
          (slib format)
          (dataframe record-types)
          (only (dataframe filter)
                dataframe-head)
          (only (dataframe helpers)
                add1
                sub1
                list-head
                transpose))

  ;; https://www.hexstreamsoft.com/articles/common-lisp-format-reference/clhs-summary/#subsections-summary-table

  (define dataframe-glimpse
    (case-lambda
      [(df) (df-glimpse-helper df 76)]
      [(df total-width) (df-glimpse-helper df total-width)]))

  (define (df-glimpse-helper df total-width)
    (check-dataframe df "(dataframe-glimpse df)")
    (let* ([slist (dataframe-slist df)]
           [df-names (dataframe-names df)]
           [df-types (prepare-df-types df)]
           [dim (dataframe-dim df)]
           [rows (car dim)]
           [n-actual (if (< rows 500) rows 500)] ;; 500 is arbitrary; just used to reduce unnecessary computation
           [ls-vals (map (lambda (series) (list-head (series-lst series) n-actual)) slist)]
           [name-width (get-width df-names 7)]
           [type-width (get-width df-types 7)]
           [list-width (- total-width (+ name-width type-width))]
           [gfs (glimpse-format-string name-width type-width list-width)]
           [list-str (map (lambda (x) (prepare-lst x list-width)) ls-vals)])
      (format #t " dim: ~d rows x ~d cols" (car dim) (cdr dim))
      (format #t gfs (build-format-list df-names df-types list-str))
      (newline)))

  (define (build-format-list df-names df-types list-str)
    (map (lambda (n t ls) (list n t ls)) df-names df-types list-str))

  ;; used for glimpse
  (define (get-width lst min-width)
    (let ([mx (apply max (map (lambda (x) (compute-object-width x 2)) lst))])
      (if (> mx min-width) mx min-width)))
  
  (define (glimpse-format-string name-width type-width list-width)
    (let* ([nw (number->string name-width)]
           [tw (number->string type-width)]
           [lw (number->string list-width)])
      (string-append "~:{~& ~" nw "a ~" tw "a ~" lw "a ~}")))

  (define (prepare-lst lst lst-width)
    ;; prepare-non-numbers comes from dataframe display and only serves to replace
    ;; nested data structures, e.g., lists, vectors, with a string representation <list>, <vector> 
    (let loop ([lst (prepare-non-numbers lst)]
               [used-width 1]  ;; starts at one for opening parenthesis
               [out-str ""])
      (cond [(null? lst)
             out-str]
            ;; the 5 is for ", ..."
            [(>= (+ used-width 5 (string-length (->string (car lst)))) lst-width)
             (string-append out-str ", ...")]
            [else
             (let* ([fill-str (if (= used-width 1) "" ", ")] ;; use space at start of all except first
                    [str (string-append fill-str (->string (car lst)))]
                    [str-width (string-length str)])
               (loop (cdr lst) (+ used-width str-width) (string-append out-str str)))])))

  (define (->string obj)
    (cond [(boolean? obj) (if obj "#t" "#f")]
          [(number? obj) (number->string obj)]
          [(symbol? obj) (symbol->string obj)]
          [(char? obj) (string obj)]
          ;; only other thing passed would be a string
          [else obj]))
  
  ;; dataframe-display is comprised of
  ;; a row with the full df dimensions
  ;; a header row with column names up to the number of columns that fit in the total-width
  ;; a row with column types surrounded by angle brackets
  ;; a table with the values for the columns that fit within the total-width

  (define dataframe-display
    (case-lambda
      [(df) (df-display-helper df 10 76 7)]
      [(df n) (df-display-helper df n 76 7)]
      [(df n total-width) (df-display-helper df n total-width 7)]
      [(df n total-width min-width) (df-display-helper df n total-width min-width)]))

  (define (df-display-helper df n total-width min-width)
    (let ([who "(dataframe-display df)"])
      (check-dataframe df who)
      (unless (> total-width min-width)
        (assertion-violation who "total-width must be greater than min-width")))
    (let* ([slist (dataframe-slist df)]
           [df-names (dataframe-names df)]
           [df-types (prepare-df-types df)]
           [dim (dataframe-dim df)]
           [rows (car dim)]
           [n-actual (if (< rows n) rows n)]
           [ls-vals (map (lambda (series) (list-head (series-lst series) n-actual)) slist)])
      (format-df df-names df-types ls-vals dim total-width min-width)))

  (define (format-df df-names df-types ls-vals dim total-width min-width)
    (let* ([prep-vals (map prepare-non-numbers ls-vals)]
           [parts (build-format-parts df-names df-types prep-vals total-width min-width 2)])
      (format #t " dim: ~d rows x ~d cols" (car dim) (cdr dim))
      ;; first item is the format directive
      ;; second item is the values being formatted
      (format #t (cadr (assoc 'header parts)) (caddr (assoc 'header parts)))
      (format #t (cadr (assoc 'types parts)) (caddr (assoc 'types parts)))
      (format #t (cadr (assoc 'table parts)) (caddr (assoc 'table parts)))
      (newline)
      (display (cdr (assoc 'footer parts)))))

  ;; wrap types in angle brackets for display
  (define (prepare-df-types df)
    (let ([df-types (map series-type (dataframe-slist df))])
      (map (lambda (x) (string-append "<" (symbol->string x) ">")) df-types)))
  
  ;; returns numeric list unchanged
  ;; elements of all other lists can be boolean, character, string, or symbol
  (define (prepare-non-numbers lst)
    (let ([types (map get-display-type lst)])
      (if (for-all number? lst)
          lst
          (map (lambda (obj typ) (get-display-value obj typ)) lst types))))
  
  ;; null? needs to be before list? b/c null is also a list
  ;; order of integer?, exact?, and number? is also important
  (define (get-display-type object)
    (let loop ([preds (list boolean? char? integer? exact-number? number? string?
                            symbol? null? list? pair? vector? dataframe? hashtable?)]
               [types '(boolean char integer exact number string
                                symbol null list pair vector dataframe hashtable)])
      (if (null? preds)
          'other
          (if ((car preds) object)
              (car types)
              (loop (cdr preds) (cdr types))))))

  (define (exact-number? object)
    (and (number? object) (exact? object)))

  (define (get-display-value object display-type)
    (cond [(member display-type '(boolean char integer exact number string symbol)) object]
          [(symbol=? display-type 'other) "<other type>"]
          [else (string-append "<" (symbol->string display-type) ">")]))

  ;; example output from build-format-parts
  ;; ((header
  ;;   "~& ~{~8@a ~7@a ~8@a ~8@a ~7@a ~8@a ~10@a ~10@a ~}"
  ;;   (Boolean Char String Symbol Exact Integer Expt Dec4))
  ;;  (types
  ;;   "~& ~{~8@a ~7@a ~8@a ~8@a ~7@a ~8@a ~10@a ~10@a ~}"
  ;;   ("<bool>" "<chr>" "<str>" "<sym>" "<num>" "<num>" "<num>"
  ;;    "<num>"))
  ;;  (table
  ;;   "~:{~& ~8@a ~7@a ~8@a ~8@a ~7@a ~8,0f ~10,3,1e ~10,4f ~}"
  ;;   ((#t #\y "these" these 1/2 1 1000000.0 132.1)
  ;;    (#f #\e "are" are 1/3 -2 -123456 -157)
  ;;    (#t #\s "strings" symbols 1/4 3 1.2346e-6 10.234)))
  ;;  (footer . " Columns not displayed: Dec2, Other\n"))
  ;;
  ;; first item of each sublist is name used (e.g., header) to extract that sublist
  ;; head is column names; types are types wrapped in angle brackets
  ;; in 8@a, the 8 is minwidth, the @ is left padding, and the a is human-readable formattting
  ;; in 10,4f, 10 is minwidth, 4 is # of digits after decimal, and f is floating point number
  ;; in 10,3,1e, 10 is minwidth, 3 is # of digits after decimal, 1 is # of digits in exponent, e is exponential number
  
  ;; build the format strings for dataframe-display
  (define (build-format-parts names df-types prep-vals total-width min-width pad)
    (let* ([e-dec 3]
           [format-parts (map (lambda (lst)
                                (compute-format-parts lst e-dec pad)) prep-vals)]
           [val-widths (map-efp format-parts 'width)]
           [col-widths (compute-column-widths names df-types val-widths min-width pad)]
           [num-types (map-efp format-parts 'num-type)]
           [declst (map-efp format-parts 'decimal)]
           [esiglst (map-efp format-parts 'esigfig)])
      (let loop ([names names]
                 [df-types df-types]
                 [prep-vals prep-vals]
                 [nt num-types]
                 [cw col-widths]
                 [dec declst]
                 [esig esiglst]
                 [used-width 0]
                 [used-names '()]
                 [used-types '()]
                 [used-vals '()]
                 [hd "~& ~{"]
                 [typ "~& ~{"]
                 [tbl "~:{~& "])
        (if (or (null? names) (>= (+ used-width (car cw)) total-width))
            ;; cons labels, e.g., 'header for lookup in format df
            (list (cons 'header (list (string-append hd "~}")
                                      (reverse used-names)))
                  (cons 'types (list (string-append typ "~}")
                                     (reverse used-types)))
                  (cons 'table (list (string-append tbl "~}")
                                     (transpose (reverse used-vals))))
                  (cons 'footer (format-footer names)))
            (let* ([width-part (string-append "~" (number->string (car cw)))]
                   [hdr-part (string-append width-part "@a ")]
                   [full-part (if (string=? (car nt) "nan")
                                  hdr-part ;; same as tbl when not numbers
                                  (string-append
                                   width-part
                                   (format-number (car nt) (car dec) (car esig))))])
              (loop (cdr names)
                    (cdr df-types)
                    (cdr prep-vals)
                    (cdr nt)
                    (cdr cw)
                    (cdr dec)
                    (cdr esig)
                    (+ used-width (car cw))
                    (cons (car names) used-names)
                    (cons (car df-types) used-types)
                    (cons (car prep-vals) used-vals)
                    ;; format directive is same for header and types
                    (string-append hd hdr-part)
                    (string-append typ hdr-part)
                    (string-append tbl full-part)))))))

  (define (compute-format-parts lst e-dec pad)
    (if (not (for-all not-fraction-number? lst))
        (let ([widthlst (map (lambda (x) (compute-object-width x pad)) lst)])
          (list (cons 'width (apply max widthlst))))
        (let* ([neg (if (any-negative? lst) 1 0)]
               [lst (map abs lst)]
               [siglst (map compute-sigfig lst)]
               [elst (map compute-expt lst)]
               [esig (apply max (map compute-sigfig elst))]
               [declst (map (lambda (x sig e)
                              (compute-decimal x sig e e-dec))
                            lst siglst elst)]
               [num-type (if (> (length (filter (lambda (x) (= x e-dec)) declst)) 0) "e" "f")]
               [widthlst (map (lambda (sig dec)
                                (compute-num-width num-type neg sig esig dec pad))
                              siglst declst)]
               [dec (if (string=? num-type "e") e-dec (apply max declst))])
          (list (cons 'num-type num-type)
                (cons 'width (apply max widthlst))
                (cons 'decimal dec)
                (cons 'esigfig esig)))))

  ;; need to make sure that exact fractions (not decimals) are sent to
  ;; compute-object-width and not compute-num-width
  (define (not-fraction-number? object)
    (or (integer? object)
        (and (number? object) (not (exact? object)))))
  
  (define (compute-num-width num-type neg sig esig dec pad)
    (if (string=? num-type "e")
        (+ neg esig dec pad 3)
        (+ neg sig dec pad)))

  ;; numbers in mixed-type columns are displayed differently than numbers in number-only columns
  ;; exact fractions are also included here rather than in compute-num-width
  ;; for reasons that I don't currently understand, this math leads to extra space in format (hence sub1)
  (define (compute-object-width object pad)
    (let ([obj-width
           (cond [(boolean? object)
                  (string-length (if object "#t" "#f"))]
                 [(char? object)
                  (string-length (string object))]
                 [(number? object)
                  (string-length (number->string object))]
                 [(string? object)
                  (string-length object)]
                 [(symbol? object)
                  (string-length (symbol->string object))]
                 [else
                  (assertion-violation
                   "(compute-width object pad)"
                   "object of this type was not anticipated")])])
      (sub1 (+ pad obj-width))))

  ;; efp = extract-extract-format parts
  (define (map-efp format-parts part)
    (map (lambda (lst) (extract-format-parts lst part)) format-parts))
  
  (define (extract-format-parts lst part)
    (if (and (not (symbol=? part 'width))
             (not (assoc 'num-type lst)))
        "nan"
        (cdr (assoc part lst))))

  (define (compute-column-widths names df-types val-widths min-width pad)
    (let ([name-widths
           (map (lambda (name) (compute-object-width name pad)) names)]
          [type-widths
           (map (lambda (df-type) (compute-object-width df-type pad)) df-types)])
      (map (lambda (name-width type-width val-width)
             (max min-width name-width type-width val-width))
           name-widths type-widths val-widths)))

  (define (any-negative? lst)
    (> (length (filter negative? lst)) 0))

  ;; e is exponential ntotation
  ;; f is floating-point notation
  (define (format-number num-type dec esig)
    (let ([dec-part (string-append "," (number->string dec))])
      (if (string=? num-type "f")
          (string-append dec-part "f ")
          (string-append dec-part "," (number->string esig) "e "))))
  
  ;; returns number of decimal digits to use
  ;; return value of 3 indicates that number should be displayed in exponential notation
  (define (compute-decimal x sigfig e e-dec)
    (let ([default 4]
          [x (abs x)])
      (cond [(or (< e -3) (> e 5)) e-dec] ;; easier to read big numbers than small numbers
            [(integer? x) 0]
            [(> e 3) 2]                   ;; fewer decimals for larger numbers 
            [(and (< x 1) (> sigfig default)) sigfig]
            [else default])))

  ;; https://github.com/r-lib/pillar/blob/master/R/sigfig.R
  (define (compute-expt x)
    (let* ([x (abs x)]
           [digits 4]
           [offset (/ (log (add1 (* -5 (expt 10 (sub1 (* -1 digits)))))) (log 10))])
      (if (zero? x)
          0
          (exact
           (floor (- (log10 x) offset))))))

  ;; digits left of decimal point
  (define (compute-sigfig x)
    (let ([x (abs x)])
      (if (zero? x)
          1
          (exact
           (add1 (floor (log10 x)))))))

  ;; noticed that (log 1000 10) returned 2.999999...
  (define (log10 x)
    (let* ([eps 1e-10]
           [logx (log x 10)]
           [rlx (round logx)])
      (if (and (> rlx (- logx eps))
               (< rlx (+ logx eps)))
          rlx
          logx)))

  ;; used for displaying column names that don't fit in the specified width
  (define (format-footer names)
    (if (null? names)
        ""
        (string-append " Columns not displayed: " (add-commas names) "\n")))
  
  ;; add commas to list of strings that are being appended; used in format-footer
  (define (add-commas ls)
    (let ([ls (map symbol->string ls)])
      (apply string-append
             (let loop ([ls (cdr ls)]
                        [results (list (car ls))])
               (if (null? ls)
                   (reverse results)
                   (loop (cdr ls) (cons (car ls) (cons ", " results))))))))

  )



