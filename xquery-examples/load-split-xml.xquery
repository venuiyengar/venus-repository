xquery version "1.0-ml";

(: Loading a document from file system with options :)
xdmp:directory-delete("/content/");
xdmp:document-load("C:\Users\viyengar\work\marklogic_massmutual\massmutual\mlcp\content\D86-final.xml",
<options xmlns="xdmp:document-load">
 <uri>/content/D86-final.xml</uri>
 <repair>none</repair>
 <format>xml</format>
 <collections>
   <collection>sample-data</collection>
   <collection>x86</collection>
 </collections>
 <permissions>{xdmp:default-permissions()}</permissions>
</options>)

(: Splits xml based on topic and inserts documents :)
let $_ := if ($delete-flag = fn:true()) then
              xdmp:directory-delete("/content/d86/")
          else ()
let $uri := "/content/D86-final.xml"
let $part := fn:doc($uri)
let $count := fn:count($part/topics/topic)
let $doc-prefix := "/content/d86/d86-0-"
let $doc-suffix := ".xml"
(: let $count := 5 :)
let $permissions := xdmp:document-get-permissions($uri)
let $collections := xdmp:document-get-collections($uri)
let $meta := 
   for $i in (1 to $count)
      let $metadata := $part/topics/topic[$i]
      let $_ := xdmp:document-insert(fn:concat($doc-prefix, $i, $doc-suffix), $metadata, $permissions, $collections)
    return $metadata
return $meta
