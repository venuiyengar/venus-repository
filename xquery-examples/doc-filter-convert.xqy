xquery version "1.0-ml";

import module namespace lnk = "http://marklogic.com/cpf/links" at "/MarkLogic/cpf/links.xqy";
import module namespace cpf = "http://marklogic.com/cpf" at "/MarkLogic/cpf/cpf.xqy";
import module namespace cvt = "http://marklogic.com/cpf/convert" at "/MarkLogic/conversion/convert.xqy";

declare namespace this = "/cpf/pipelines/doc-filter-convert.xqy";
declare namespace html = "http://www.w3.org/1999/xhtml";

declare variable $cpf:document-uri as xs:string external;
declare variable $cpf:transition as node() external;
declare variable $cpf:options as node() external;


declare function this:metadata($filter-output) {
   let $body := $filter-output/html:body/*
   let $meta := 
     for $meta in $filter-output/html:head/html:meta
     let $qname := xs:QName(fn:replace($meta/@name,"[^\i\c]",""))
     return 
       element {$qname} {
         fn:data($meta/@content)
       }
   return
     <documentMetadata> {
        <metadata>{$meta}</metadata>,
        <textContent>{$body}</textContent>
     }</documentMetadata>
};


(:  main  :)

if (cpf:check-transition($cpf:document-uri,$cpf:transition)) then
    try {
        let $dest-root-option := fn:string($cpf:options//this:destination-root)

        let $destination-collections :=
            if (fn:empty($cpf:options//this:destination-collection/text()))
            then ()
            else fn:data($cpf:options//this:destination-collection)

        let $destination-root :=
            if ( $dest-root-option ne "" ) then
                fn:concat( $dest-root-option,
                        if (fn:ends-with($dest-root-option, "/")) then "" else "/",
                        cvt:basename($cpf:document-uri) )
            else $cpf:document-uri
        let $destination-uri := fn:concat($destination-root,".xhtml")

  
        let $uri  := $cpf:document-uri 
        let $filename := fn:tokenize($uri,"\\|/")[fn:last()]
        let $part := fn:doc($cpf:document-uri)
        let $filtered-doc := xdmp:document-filter($part)//html:html
        let $meta := 
          <documentMetadata>
          <filename>{$filename}</filename>
          <resourcePath>{$cpf:document-uri}</resourcePath>
          {this:metadata($filtered-doc)/*}
          </documentMetadata>
        let $converted := 
            try {
                switch($meta//content-type)
                   case "application/pdf" return xdmp:pdf-convert($part,$filename,())
                   (:We need to check for 2007+ version since they get reported under wrong mimetype:)
                   (: BC: done for word/powerpoint, need to check on excel :)
                   case "application/msword" return xdmp:word-convert($part,$filename,())
                   case "application/vnd.openxmlformats-officedocument.wordprocessingml.document" return xdmp:word-convert($part,$filename,())
                   case "application/vnd.ms-excel" return xdmp:excel-convert($part,$filename,())
                   case "application/vnd.openxmlformats-officedocument.presentationml.presentation" return xdmp:excel-convert($part,$filename,())
                   case "application/vnd.ms-powerpoint" return xdmp:powerpoint-convert($part,$filename,())
				           case "application/xml" return $part
                   default return () 
            } catch($ex) {
                xdmp:log(xs:QName("CONVERSION-ERROR"),"Could not convert"|| $filename || $ex/error:format-string),
                ()
            }
 
        let $support-files :=
            if($converted) then
                let $conv-man := $converted[1]/*:part
                let $conv-parts := $converted[2 to fn:last()]
                for $conv-part at $pos in $conv-parts
                return
                    xdmp:document-insert(fn:concat("/Resources/",$uri,"/",$conv-man[$pos]),$conv-part)
            else ()

        let $permissions :=
            if (fn:exists(fn:doc($destination-uri)))
            then xdmp:document-get-permissions($destination-uri)
            else xdmp:document-get-permissions($cpf:document-uri)

        let $collections :=
            fn:distinct-values(
                    ( if (fn:exists(fn:doc($destination-uri)))
                    then xdmp:document-get-collections($destination-uri)
                    else xdmp:default-collections(),
                    $destination-collections,
                    xdmp:document-get-collections($cpf:document-uri) (: carry over permissions from the binary doc :)
                    )
            )

        return (
            xdmp:document-insert(fn:concat("/Resources",if(fn:starts-with($uri,"/")) then "" else "/", $uri,"/metadata.xml"), $meta, $permissions, $collections),

            if ( ($dest-root-option ne "") and ($destination-uri ne $cpf:document-uri) ) then
                lnk:create( $destination-uri, $cpf:document-uri,
                        "source", "filter", "strong" )
            else ()
        )

        ,
        cpf:success( $cpf:document-uri, $cpf:transition, () )
    }
    catch ($e) {
        cpf:failure( $cpf:document-uri, $cpf:transition, $e, () )
    }
else ()
