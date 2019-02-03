# API Docs - v1.0.0-SNAPSHOT

## Beam

### groupbykey *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processor">(Stream Processor)</a>*

<p style="word-wrap: break-word">This stream processor extension performs grouping of events by key for WindowedValue objects when executing a Beam pipeline.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
beam:groupbykey(<OBJECT> event)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">event</td>
        <td style="vertical-align: top; word-wrap: break-word">All the events of type WindowedValue arriving in chunk to execute GroupByKey transform</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream inputStream (event object);
@info(name = 'query1')
from inputStream#beam:groupbykey(event)
select event
insert into outputStream;
```
<p style="word-wrap: break-word">This query performs Beam GroupByKey transformation provided WindowedValue&lt;KV&gt; as event</p>

### pardo *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processor">(Stream Processor)</a>*

<p style="word-wrap: break-word">This stream processor extension extracts values from .<br>&nbsp;WindowedValue objects to pass into Siddhi sink stream.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
beam:pardo(<OBJECT> event)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">event</td>
        <td style="vertical-align: top; word-wrap: break-word">All WindowedValue&lt;String&gt; objects that will be sent to file sink</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream inputStream (event object);
@sink(type='file', file.uri='/destination', append='true', @map(type='text', @payload('{{value}}') ))
define stream outputStream
@info(name = 'query1')
from inputStream#beam:sink(event)
select value, 
insert into outputStream;
```
<p style="word-wrap: break-word">This query will extract String value from event and sent to file sink stream</p>

### pardo *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processor">(Stream Processor)</a>*

<p style="word-wrap: break-word">This stream processor extension performs ParDo transformation.<br>&nbsp;for WindowedValue objects when executing a Beam pipeline.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
beam:pardo(<OBJECT> event)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">event</td>
        <td style="vertical-align: top; word-wrap: break-word">All the events of type WindowedValue arriving in chunk to execute ParDo transform</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream inputStream (event object);
@info(name = 'query1')
from inputStream#beam:pardo(event)
select event
insert into outputStream;
```
<p style="word-wrap: break-word">This query performs Beam ParDo transformation to all events arriving to inputStream</p>

