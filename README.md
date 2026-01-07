# Freely TV Guide Splitter


Fetch the Freely TV-Guide API and publish **per‑channel JSON** files you can point Home Assistant REST sensors at (with a compatibility block mimicking the old Freesat/Freeview card shape).

## Preview

![Home Assistant EPG card](https://github.com/elyobelyob/freely_tv_guide/blob/1cfc53f9a20dbb4fdef2301da48d4b39d1229a4f/docs/img/readme/Screenshot%202025-10-03%20at%2016.27.32.png)

## Home Assistant examples
Replace:

<CHANNEL_ID> with e.g. 37124

<SENSOR_NAME> with e.g. EPG_BBCOne

<sensor_name> in the Lovelace card with the entity id (lowercased by HA, e.g. sensor.epg_bbcone).

### REST sensor (drop in `sensors.yaml` or under `sensor:` in `configuration.yaml`)
```yaml
# Replace <CHANNEL_ID> with the JSON you want (e.g. 37124)
# Replace <SENSOR_NAME> if you want a different entity id
- platform: rest
  resource: "https://elyobelyob.github.io/freely_tv_guide/channels/<CHANNEL_ID>.json"
  scan_interval: 28800
  name: <SENSOR_NAME>
  value_template: >-
    {%- macro to_minutes(d) -%}
      {%- if d is number -%}{{ d }}
      {%- elif d is string and d.startswith('PT') -%}
        {% set h = ('H' in d) and (d.split('T')[-1].split('H')[0] | int) or 0 %}
        {% set m = ('M' in d) and (d.split('H')[-1].split('M')[0] | int) or 0 %}
        {% set s = ('S' in d) and (d.split('M')[-1].split('S')[0] | int) or 0 %}
        {{ h*60 + m + (s // 60) }}
      {%- else -%}0{%- endif -%}
    {%- endmacro -%}
    {% set evts = value_json.compat.freesat_card[0].event | default([], true) %}
    {% set nowts = now() %}
    {% for e in (evts | sort(attribute='startTime')) %}
      {% set st = as_local(as_datetime(e.startTime)) %}
      {% set en = st + timedelta(minutes=(to_minutes(e.duration) | int)) %}
      {% if en > nowts %}
        {{ st.strftime("%H:%M") }}  : {{ e.name }}
        
        {{ e.description or "" }}
        {% break %}
      {% endif %}
    {% else %}
      No EPG
    {% endfor %}
  json_attributes_path: "$.compat.freesat_card.0"
  json_attributes:
    - event
```

### Lovelace (compact list using a Markdown card)
```yaml
type: vertical-stack
cards:
  - type: entities
    entities:
      - type: custom:hui-element
        card_type: markdown
        content: |-
          {% set evts = state_attr('sensor.<sensor_name>','event') or [] %}
          {% set nowts = now() %}
          {% set ns = namespace(count=0) %}
          {%- for e in evts | sort(attribute='startTime') -%}
            {%- set st = as_local(as_datetime(e.startTime)) -%}
            {%- set en = st + timedelta(minutes=(e.duration | int(0))) -%}
            {%- if en > nowts -%}
              {%- if ns.count > 0 -%}<br>{%- endif -%}
              {{ st.strftime("%H:%M") }} : {{ e.name }}
              {%- set ns.count = ns.count + 1 -%}
            {%- endif -%}
          {%- endfor -%}
          {%- if ns.count == 0 -%}
            No upcoming programmes
          {%- endif -%}
```

## Home Assistant Examples

### Update EPG Sensors Button

You can add a simple Lovelace button to manually refresh all EPG (Electronic Programme Guide) sensors without waiting for the next automatic update.  
This is useful after restarting Home Assistant or when you’ve just updated the guide data and want to pull in the latest information immediately.

```yaml
show_name: true
show_icon: true
type: button
name: Update EPG Sensors
tap_action:
  action: call-service
  service: homeassistant.update_entity
  service_data:
    entity_id:
      - sensor.epg_bbcone
      - sensor.epg_bbctwo
      - sensor.epg_itv
      - sensor.epg_channel4
      - sensor.epg_channel5
      - sensor.epg_bbc_news
      - sensor.epg_bbc_3_hd
      - sensor.epg_bbc_4_hd
icon: mdi:refresh
layout_options:
  grid_columns: 1
  grid_rows: 2
```

## Channel list
<!-- CHANNELS_START -->
| ID | Name | JSON |
|---:|---|---|
| 38051 | 4Homes | [channels/38051.json](https://elyobelyob.github.io/freely_tv_guide/channels/38051.json) |
| 38050 | 4Life | [channels/38050.json](https://elyobelyob.github.io/freely_tv_guide/channels/38050.json) |
| 38049 | 4Reality | [channels/38049.json](https://elyobelyob.github.io/freely_tv_guide/channels/38049.json) |
| 37899 | 4seven | [channels/37899.json](https://elyobelyob.github.io/freely_tv_guide/channels/37899.json) |
| 38145 | 5 | [channels/38145.json](https://elyobelyob.github.io/freely_tv_guide/channels/38145.json) |
| 13024 | 5 +1 | [channels/13024.json](https://elyobelyob.github.io/freely_tv_guide/channels/13024.json) |
| 38146 | 5ACTION | [channels/38146.json](https://elyobelyob.github.io/freely_tv_guide/channels/38146.json) |
| 38147 | 5SELECT | [channels/38147.json](https://elyobelyob.github.io/freely_tv_guide/channels/38147.json) |
| 38148 | 5STAR | [channels/38148.json](https://elyobelyob.github.io/freely_tv_guide/channels/38148.json) |
| 38149 | 5USA | [channels/38149.json](https://elyobelyob.github.io/freely_tv_guide/channels/38149.json) |
| 34240 | AL ARABIYA | [channels/34240.json](https://elyobelyob.github.io/freely_tv_guide/channels/34240.json) |
| 34304 | ASHARQ NEWS | [channels/34304.json](https://elyobelyob.github.io/freely_tv_guide/channels/34304.json) |
| 16284 | Al Jazeera Arabic | [channels/16284.json](https://elyobelyob.github.io/freely_tv_guide/channels/16284.json) |
| 16278 | Al Jazeera English | [channels/16278.json](https://elyobelyob.github.io/freely_tv_guide/channels/16278.json) |
| 16270 | Alaraby Network | [channels/16270.json](https://elyobelyob.github.io/freely_tv_guide/channels/16270.json) |
| 16346 | Amazing Facts | [channels/16346.json](https://elyobelyob.github.io/freely_tv_guide/channels/16346.json) |
| 37259 | BBC ALBA | [channels/37259.json](https://elyobelyob.github.io/freely_tv_guide/channels/37259.json) |
| 37405 | BBC Asian Network | [channels/37405.json](https://elyobelyob.github.io/freely_tv_guide/channels/37405.json) |
| 37571 | BBC Berkshire | [channels/37571.json](https://elyobelyob.github.io/freely_tv_guide/channels/37571.json) |
| 37441 | BBC Bristol | [channels/37441.json](https://elyobelyob.github.io/freely_tv_guide/channels/37441.json) |
| 37541 | BBC CWR | [channels/37541.json](https://elyobelyob.github.io/freely_tv_guide/channels/37541.json) |
| 37581 | BBC Cambridge | [channels/37581.json](https://elyobelyob.github.io/freely_tv_guide/channels/37581.json) |
| 37442 | BBC Cornwall | [channels/37442.json](https://elyobelyob.github.io/freely_tv_guide/channels/37442.json) |
| 37513 | BBC Cumbria | [channels/37513.json](https://elyobelyob.github.io/freely_tv_guide/channels/37513.json) |
| 37510 | BBC Derby | [channels/37510.json](https://elyobelyob.github.io/freely_tv_guide/channels/37510.json) |
| 37506 | BBC Devon | [channels/37506.json](https://elyobelyob.github.io/freely_tv_guide/channels/37506.json) |
| 37575 | BBC Essex | [channels/37575.json](https://elyobelyob.github.io/freely_tv_guide/channels/37575.json) |
| 37274 | BBC Four | [channels/37274.json](https://elyobelyob.github.io/freely_tv_guide/channels/37274.json) |
| 37505 | BBC Gloucestershire | [channels/37505.json](https://elyobelyob.github.io/freely_tv_guide/channels/37505.json) |
| 37454 | BBC Guernsey | [channels/37454.json](https://elyobelyob.github.io/freely_tv_guide/channels/37454.json) |
| 37573 | BBC H&W | [channels/37573.json](https://elyobelyob.github.io/freely_tv_guide/channels/37573.json) |
| 37519 | BBC Humberside | [channels/37519.json](https://elyobelyob.github.io/freely_tv_guide/channels/37519.json) |
| 37518 | BBC Jersey | [channels/37518.json](https://elyobelyob.github.io/freely_tv_guide/channels/37518.json) |
| 37452 | BBC Kent | [channels/37452.json](https://elyobelyob.github.io/freely_tv_guide/channels/37452.json) |
| 37512 | BBC Lancashire | [channels/37512.json](https://elyobelyob.github.io/freely_tv_guide/channels/37512.json) |
| 37450 | BBC Leeds | [channels/37450.json](https://elyobelyob.github.io/freely_tv_guide/channels/37450.json) |
| 37574 | BBC Leicester | [channels/37574.json](https://elyobelyob.github.io/freely_tv_guide/channels/37574.json) |
| 37455 | BBC Lincolnshire | [channels/37455.json](https://elyobelyob.github.io/freely_tv_guide/channels/37455.json) |
| 37558 | BBC Manchester | [channels/37558.json](https://elyobelyob.github.io/freely_tv_guide/channels/37558.json) |
| 37576 | BBC Merseyside | [channels/37576.json](https://elyobelyob.github.io/freely_tv_guide/channels/37576.json) |
| 37449 | BBC Newcastle | [channels/37449.json](https://elyobelyob.github.io/freely_tv_guide/channels/37449.json) |
| 37252 | BBC News | [channels/37252.json](https://elyobelyob.github.io/freely_tv_guide/channels/37252.json) |
| 37447 | BBC Norfolk | [channels/37447.json](https://elyobelyob.github.io/freely_tv_guide/channels/37447.json) |
| 37517 | BBC Northampton | [channels/37517.json](https://elyobelyob.github.io/freely_tv_guide/channels/37517.json) |
| 37446 | BBC Nottingham | [channels/37446.json](https://elyobelyob.github.io/freely_tv_guide/channels/37446.json) |
| 37134 | BBC One CI | [channels/37134.json](https://elyobelyob.github.io/freely_tv_guide/channels/37134.json) |
| 37126 | BBC One E Mids | [channels/37126.json](https://elyobelyob.github.io/freely_tv_guide/channels/37126.json) |
| 37127 | BBC One East | [channels/37127.json](https://elyobelyob.github.io/freely_tv_guide/channels/37127.json) |
| 37124 | BBC One London | [channels/37124.json](https://elyobelyob.github.io/freely_tv_guide/channels/37124.json) |
| 37129 | BBC One NE&C | [channels/37129.json](https://elyobelyob.github.io/freely_tv_guide/channels/37129.json) |
| 37181 | BBC One NI | [channels/37181.json](https://elyobelyob.github.io/freely_tv_guide/channels/37181.json) |
| 37128 | BBC One NW | [channels/37128.json](https://elyobelyob.github.io/freely_tv_guide/channels/37128.json) |
| 37132 | BBC One SE | [channels/37132.json](https://elyobelyob.github.io/freely_tv_guide/channels/37132.json) |
| 37122 | BBC One SW | [channels/37122.json](https://elyobelyob.github.io/freely_tv_guide/channels/37122.json) |
| 37180 | BBC One Scotland | [channels/37180.json](https://elyobelyob.github.io/freely_tv_guide/channels/37180.json) |
| 37123 | BBC One South | [channels/37123.json](https://elyobelyob.github.io/freely_tv_guide/channels/37123.json) |
| 37125 | BBC One W Mids | [channels/37125.json](https://elyobelyob.github.io/freely_tv_guide/channels/37125.json) |
| 37182 | BBC One Wales | [channels/37182.json](https://elyobelyob.github.io/freely_tv_guide/channels/37182.json) |
| 37121 | BBC One West | [channels/37121.json](https://elyobelyob.github.io/freely_tv_guide/channels/37121.json) |
| 37135 | BBC One Yk&Li | [channels/37135.json](https://elyobelyob.github.io/freely_tv_guide/channels/37135.json) |
| 37130 | BBC One Yorks | [channels/37130.json](https://elyobelyob.github.io/freely_tv_guide/channels/37130.json) |
| 37451 | BBC Oxford | [channels/37451.json](https://elyobelyob.github.io/freely_tv_guide/channels/37451.json) |
| 37258 | BBC Parliament | [channels/37258.json](https://elyobelyob.github.io/freely_tv_guide/channels/37258.json) |
| 37409 | BBC R n Gaidheal | [channels/37409.json](https://elyobelyob.github.io/freely_tv_guide/channels/37409.json) |
| 37417 | BBC Radio 1 | [channels/37417.json](https://elyobelyob.github.io/freely_tv_guide/channels/37417.json) |
| 37404 | BBC Radio 1Xtra | [channels/37404.json](https://elyobelyob.github.io/freely_tv_guide/channels/37404.json) |
| 37418 | BBC Radio 2 | [channels/37418.json](https://elyobelyob.github.io/freely_tv_guide/channels/37418.json) |
| 37419 | BBC Radio 3 | [channels/37419.json](https://elyobelyob.github.io/freely_tv_guide/channels/37419.json) |
| 37420 | BBC Radio 4 | [channels/37420.json](https://elyobelyob.github.io/freely_tv_guide/channels/37420.json) |
| 37403 | BBC Radio 4 Extra | [channels/37403.json](https://elyobelyob.github.io/freely_tv_guide/channels/37403.json) |
| 37400 | BBC Radio 5 Live | [channels/37400.json](https://elyobelyob.github.io/freely_tv_guide/channels/37400.json) |
| 37401 | BBC Radio 5 Sports Extra | [channels/37401.json](https://elyobelyob.github.io/freely_tv_guide/channels/37401.json) |
| 37402 | BBC Radio 6 Music | [channels/37402.json](https://elyobelyob.github.io/freely_tv_guide/channels/37402.json) |
| 37413 | BBC Radio Cymru | [channels/37413.json](https://elyobelyob.github.io/freely_tv_guide/channels/37413.json) |
| 37414 | BBC Radio Cymru 2 | [channels/37414.json](https://elyobelyob.github.io/freely_tv_guide/channels/37414.json) |
| 37411 | BBC Radio Foyle | [channels/37411.json](https://elyobelyob.github.io/freely_tv_guide/channels/37411.json) |
| 37444 | BBC Radio London | [channels/37444.json](https://elyobelyob.github.io/freely_tv_guide/channels/37444.json) |
| 37408 | BBC Radio Scotland | [channels/37408.json](https://elyobelyob.github.io/freely_tv_guide/channels/37408.json) |
| 37410 | BBC Radio Ulster | [channels/37410.json](https://elyobelyob.github.io/freely_tv_guide/channels/37410.json) |
| 37412 | BBC Radio Wales | [channels/37412.json](https://elyobelyob.github.io/freely_tv_guide/channels/37412.json) |
| 4416 | BBC Red Button | [channels/4416.json](https://elyobelyob.github.io/freely_tv_guide/channels/4416.json) |
| 37260 | BBC Scotland | [channels/37260.json](https://elyobelyob.github.io/freely_tv_guide/channels/37260.json) |
| 37514 | BBC Sheffield | [channels/37514.json](https://elyobelyob.github.io/freely_tv_guide/channels/37514.json) |
| 37477 | BBC Shropshire | [channels/37477.json](https://elyobelyob.github.io/freely_tv_guide/channels/37477.json) |
| 37443 | BBC Solent | [channels/37443.json](https://elyobelyob.github.io/freely_tv_guide/channels/37443.json) |
| 37507 | BBC Solent Dorset | [channels/37507.json](https://elyobelyob.github.io/freely_tv_guide/channels/37507.json) |
| 37473 | BBC Somerset | [channels/37473.json](https://elyobelyob.github.io/freely_tv_guide/channels/37473.json) |
| 37509 | BBC Stoke | [channels/37509.json](https://elyobelyob.github.io/freely_tv_guide/channels/37509.json) |
| 37511 | BBC Suffolk | [channels/37511.json](https://elyobelyob.github.io/freely_tv_guide/channels/37511.json) |
| 37580 | BBC Surrey | [channels/37580.json](https://elyobelyob.github.io/freely_tv_guide/channels/37580.json) |
| 37516 | BBC Sussex | [channels/37516.json](https://elyobelyob.github.io/freely_tv_guide/channels/37516.json) |
| 37577 | BBC Tees | [channels/37577.json](https://elyobelyob.github.io/freely_tv_guide/channels/37577.json) |
| 37272 | BBC Three | [channels/37272.json](https://elyobelyob.github.io/freely_tv_guide/channels/37272.json) |
| 37453 | BBC Three Counties | [channels/37453.json](https://elyobelyob.github.io/freely_tv_guide/channels/37453.json) |
| 37184 | BBC Two | [channels/37184.json](https://elyobelyob.github.io/freely_tv_guide/channels/37184.json) |
| 37245 | BBC Two NI | [channels/37245.json](https://elyobelyob.github.io/freely_tv_guide/channels/37245.json) |
| 37246 | BBC Two Wales | [channels/37246.json](https://elyobelyob.github.io/freely_tv_guide/channels/37246.json) |
| 37445 | BBC WM | [channels/37445.json](https://elyobelyob.github.io/freely_tv_guide/channels/37445.json) |
| 37569 | BBC Wiltshire | [channels/37569.json](https://elyobelyob.github.io/freely_tv_guide/channels/37569.json) |
| 37406 | BBC World Service | [channels/37406.json](https://elyobelyob.github.io/freely_tv_guide/channels/37406.json) |
| 37578 | BBC York | [channels/37578.json](https://elyobelyob.github.io/freely_tv_guide/channels/37578.json) |
| 14384 | Blaze | [channels/14384.json](https://elyobelyob.github.io/freely_tv_guide/channels/14384.json) |
| 14388 | Blaze+1 | [channels/14388.json](https://elyobelyob.github.io/freely_tv_guide/channels/14388.json) |
| 37275 | CBBC | [channels/37275.json](https://elyobelyob.github.io/freely_tv_guide/channels/37275.json) |
| 37276 | CBeebies | [channels/37276.json](https://elyobelyob.github.io/freely_tv_guide/channels/37276.json) |
| 16164 | CNA Originals | [channels/16164.json](https://elyobelyob.github.io/freely_tv_guide/channels/16164.json) |
| 39425 | CNN Headlines | [channels/39425.json](https://elyobelyob.github.io/freely_tv_guide/channels/39425.json) |
| 22688 | Capital | [channels/22688.json](https://elyobelyob.github.io/freely_tv_guide/channels/22688.json) |
| 34432 | Cartoon Classics | [channels/34432.json](https://elyobelyob.github.io/freely_tv_guide/channels/34432.json) |
| 22226 | Challenge | [channels/22226.json](https://elyobelyob.github.io/freely_tv_guide/channels/22226.json) |
| 37889 | Channel 4 | [channels/37889.json](https://elyobelyob.github.io/freely_tv_guide/channels/37889.json) |
| 8452 | Channel 4+1 | [channels/8452.json](https://elyobelyob.github.io/freely_tv_guide/channels/8452.json) |
| 15448 | Channelbox | [channels/15448.json](https://elyobelyob.github.io/freely_tv_guide/channels/15448.json) |
| 22848 | Classic FM | [channels/22848.json](https://elyobelyob.github.io/freely_tv_guide/channels/22848.json) |
| 27680 | DMAX | [channels/27680.json](https://elyobelyob.github.io/freely_tv_guide/channels/27680.json) |
| 37900 | E4 | [channels/37900.json](https://elyobelyob.github.io/freely_tv_guide/channels/37900.json) |
| 22368 | E4 Extra | [channels/22368.json](https://elyobelyob.github.io/freely_tv_guide/channels/22368.json) |
| 8458 | E4+1 | [channels/8458.json](https://elyobelyob.github.io/freely_tv_guide/channels/8458.json) |
| 16180 | EuroNews | [channels/16180.json](https://elyobelyob.github.io/freely_tv_guide/channels/16180.json) |
| 16370 | FRANCE 24 | [channels/16370.json](https://elyobelyob.github.io/freely_tv_guide/channels/16370.json) |
| 37901 | Film4 | [channels/37901.json](https://elyobelyob.github.io/freely_tv_guide/channels/37901.json) |
| 22464 | Film4+1 | [channels/22464.json](https://elyobelyob.github.io/freely_tv_guide/channels/22464.json) |
| 23040 | Food Network | [channels/23040.json](https://elyobelyob.github.io/freely_tv_guide/channels/23040.json) |
| 27360 | GB News | [channels/27360.json](https://elyobelyob.github.io/freely_tv_guide/channels/27360.json) |
| 16170 | GIGS | [channels/16170.json](https://elyobelyob.github.io/freely_tv_guide/channels/16170.json) |
| 39201 | Gemporia | [channels/39201.json](https://elyobelyob.github.io/freely_tv_guide/channels/39201.json) |
| 16286 | Global Arabic + | [channels/16286.json](https://elyobelyob.github.io/freely_tv_guide/channels/16286.json) |
| 16256 | God TV | [channels/16256.json](https://elyobelyob.github.io/freely_tv_guide/channels/16256.json) |
| 15576 | Great! Action | [channels/15576.json](https://elyobelyob.github.io/freely_tv_guide/channels/15576.json) |
| 15584 | Great! Movies | [channels/15584.json](https://elyobelyob.github.io/freely_tv_guide/channels/15584.json) |
| 27872 | Great! Mystery | [channels/27872.json](https://elyobelyob.github.io/freely_tv_guide/channels/27872.json) |
| 28352 | Great! Player | [channels/28352.json](https://elyobelyob.github.io/freely_tv_guide/channels/28352.json) |
| 27296 | Great! Romance | [channels/27296.json](https://elyobelyob.github.io/freely_tv_guide/channels/27296.json) |
| 28384 | Great! Romance Mix | [channels/28384.json](https://elyobelyob.github.io/freely_tv_guide/channels/28384.json) |
| 27168 | Great! TV | [channels/27168.json](https://elyobelyob.github.io/freely_tv_guide/channels/27168.json) |
| 28000 | HGTV | [channels/28000.json](https://elyobelyob.github.io/freely_tv_guide/channels/28000.json) |
| 22656 | Heart | [channels/22656.json](https://elyobelyob.github.io/freely_tv_guide/channels/22656.json) |
| 34176 | High Street TV | [channels/34176.json](https://elyobelyob.github.io/freely_tv_guide/channels/34176.json) |
| 39203 | HobbyMaker | [channels/39203.json](https://elyobelyob.github.io/freely_tv_guide/channels/39203.json) |
| 27232 | Hobbycraft TV | [channels/27232.json](https://elyobelyob.github.io/freely_tv_guide/channels/27232.json) |
| 37655 | ITV Quiz | [channels/37655.json](https://elyobelyob.github.io/freely_tv_guide/channels/37655.json) |
| 37641 | ITV1 | [channels/37641.json](https://elyobelyob.github.io/freely_tv_guide/channels/37641.json) |
| 8361 | ITV1+1 | [channels/8361.json](https://elyobelyob.github.io/freely_tv_guide/channels/8361.json) |
| 37652 | ITV2 | [channels/37652.json](https://elyobelyob.github.io/freely_tv_guide/channels/37652.json) |
| 15952 | ITV2+1 | [channels/15952.json](https://elyobelyob.github.io/freely_tv_guide/channels/15952.json) |
| 37653 | ITV3 | [channels/37653.json](https://elyobelyob.github.io/freely_tv_guide/channels/37653.json) |
| 16016 | ITV3+1 | [channels/16016.json](https://elyobelyob.github.io/freely_tv_guide/channels/16016.json) |
| 37654 | ITV4 | [channels/37654.json](https://elyobelyob.github.io/freely_tv_guide/channels/37654.json) |
| 15920 | ITV4+1 | [channels/15920.json](https://elyobelyob.github.io/freely_tv_guide/channels/15920.json) |
| 25920 | Ideal World | [channels/25920.json](https://elyobelyob.github.io/freely_tv_guide/channels/25920.json) |
| 33280 | India Today | [channels/33280.json](https://elyobelyob.github.io/freely_tv_guide/channels/33280.json) |
| 33984 | It Is Written TV | [channels/33984.json](https://elyobelyob.github.io/freely_tv_guide/channels/33984.json) |
| 39202 | Jewellery Maker | [channels/39202.json](https://elyobelyob.github.io/freely_tv_guide/channels/39202.json) |
| 16200 | Ketchup TV | [channels/16200.json](https://elyobelyob.github.io/freely_tv_guide/channels/16200.json) |
| 16216 | Ketchup Too | [channels/16216.json](https://elyobelyob.github.io/freely_tv_guide/channels/16216.json) |
| 22880 | LBC | [channels/22880.json](https://elyobelyob.github.io/freely_tv_guide/channels/22880.json) |
| 14480 | LEGEND | [channels/14480.json](https://elyobelyob.github.io/freely_tv_guide/channels/14480.json) |
| 20032 | LEGEND XTRA | [channels/20032.json](https://elyobelyob.github.io/freely_tv_guide/channels/20032.json) |
| 32842 | London TV | [channels/32842.json](https://elyobelyob.github.io/freely_tv_guide/channels/32842.json) |
| 13144 | MBC | [channels/13144.json](https://elyobelyob.github.io/freely_tv_guide/channels/13144.json) |
| 33664 | MBC Group | [channels/33664.json](https://elyobelyob.github.io/freely_tv_guide/channels/33664.json) |
| 37902 | More4 | [channels/37902.json](https://elyobelyob.github.io/freely_tv_guide/channels/37902.json) |
| 23680 | Must Have Ideas | [channels/23680.json](https://elyobelyob.github.io/freely_tv_guide/channels/23680.json) |
| 16322 | NHK WORLD | [channels/16322.json](https://elyobelyob.github.io/freely_tv_guide/channels/16322.json) |
| 16360 | New Media TV | [channels/16360.json](https://elyobelyob.github.io/freely_tv_guide/channels/16360.json) |
| 16328 | Newsmax | [channels/16328.json](https://elyobelyob.github.io/freely_tv_guide/channels/16328.json) |
| 34560 | Nolly Africa | [channels/34560.json](https://elyobelyob.github.io/freely_tv_guide/channels/34560.json) |
| 16376 | Nosey | [channels/16376.json](https://elyobelyob.github.io/freely_tv_guide/channels/16376.json) |
| 34048 | OUTflix Proud | [channels/34048.json](https://elyobelyob.github.io/freely_tv_guide/channels/34048.json) |
| 16194 | Odyssey TV | [channels/16194.json](https://elyobelyob.github.io/freely_tv_guide/channels/16194.json) |
| 15064 | On Demand 365 | [channels/15064.json](https://elyobelyob.github.io/freely_tv_guide/channels/15064.json) |
| 27968 | PBS America | [channels/27968.json](https://elyobelyob.github.io/freely_tv_guide/channels/27968.json) |
| 16272 | POP | [channels/16272.json](https://elyobelyob.github.io/freely_tv_guide/channels/16272.json) |
| 27424 | POP UP | [channels/27424.json](https://elyobelyob.github.io/freely_tv_guide/channels/27424.json) |
| 28160 | Premier Radio | [channels/28160.json](https://elyobelyob.github.io/freely_tv_guide/channels/28160.json) |
| 16400 | Purpose Media | [channels/16400.json](https://elyobelyob.github.io/freely_tv_guide/channels/16400.json) |
| 27328 | QUEST | [channels/27328.json](https://elyobelyob.github.io/freely_tv_guide/channels/27328.json) |
| 23808 | QUEST+1 | [channels/23808.json](https://elyobelyob.github.io/freely_tv_guide/channels/23808.json) |
| 13120 | QVC | [channels/13120.json](https://elyobelyob.github.io/freely_tv_guide/channels/13120.json) |
| 14416 | QVC2 | [channels/14416.json](https://elyobelyob.github.io/freely_tv_guide/channels/14416.json) |
| 23744 | Quest Red | [channels/23744.json](https://elyobelyob.github.io/freely_tv_guide/channels/23744.json) |
| 22624 | RNIB Connect | [channels/22624.json](https://elyobelyob.github.io/freely_tv_guide/channels/22624.json) |
| 16240 | ROK | [channels/16240.json](https://elyobelyob.github.io/freely_tv_guide/channels/16240.json) |
| 23712 | Really | [channels/23712.json](https://elyobelyob.github.io/freely_tv_guide/channels/23712.json) |
| 16248 | Revelation TV | [channels/16248.json](https://elyobelyob.github.io/freely_tv_guide/channels/16248.json) |
| 27744 | Rewind TV | [channels/27744.json](https://elyobelyob.github.io/freely_tv_guide/channels/27744.json) |
| 38113 | S4C | [channels/38113.json](https://elyobelyob.github.io/freely_tv_guide/channels/38113.json) |
| 37697 | Saturday Night Every Night | [channels/37697.json](https://elyobelyob.github.io/freely_tv_guide/channels/37697.json) |
| 16394 | Shop On TV | [channels/16394.json](https://elyobelyob.github.io/freely_tv_guide/channels/16394.json) |
| 22144 | Sky Arts | [channels/22144.json](https://elyobelyob.github.io/freely_tv_guide/channels/22144.json) |
| 22208 | Sky Mix | [channels/22208.json](https://elyobelyob.github.io/freely_tv_guide/channels/22208.json) |
| 22080 | Sky News | [channels/22080.json](https://elyobelyob.github.io/freely_tv_guide/channels/22080.json) |
| 26368 | Smooth Radio | [channels/26368.json](https://elyobelyob.github.io/freely_tv_guide/channels/26368.json) |
| 23184 | SonLife | [channels/23184.json](https://elyobelyob.github.io/freely_tv_guide/channels/23184.json) |
| 15016 | Sonlife | [channels/15016.json](https://elyobelyob.github.io/freely_tv_guide/channels/15016.json) |
| 20160 | TBN UK | [channels/20160.json](https://elyobelyob.github.io/freely_tv_guide/channels/20160.json) |
| 15592 | TJC | [channels/15592.json](https://elyobelyob.github.io/freely_tv_guide/channels/15592.json) |
| 14448 | TRUE CRIME | [channels/14448.json](https://elyobelyob.github.io/freely_tv_guide/channels/14448.json) |
| 14456 | TRUE CRIME XTRA | [channels/14456.json](https://elyobelyob.github.io/freely_tv_guide/channels/14456.json) |
| 16406 | Talk | [channels/16406.json](https://elyobelyob.github.io/freely_tv_guide/channels/16406.json) |
| 28224 | TalkingPictures TV | [channels/28224.json](https://elyobelyob.github.io/freely_tv_guide/channels/28224.json) |
| 23904 | That's 80s | [channels/23904.json](https://elyobelyob.github.io/freely_tv_guide/channels/23904.json) |
| 28032 | That's Melody | [channels/28032.json](https://elyobelyob.github.io/freely_tv_guide/channels/28032.json) |
| 15864 | That's Oldies | [channels/15864.json](https://elyobelyob.github.io/freely_tv_guide/channels/15864.json) |
| 27936 | That's TV (UK) | [channels/27936.json](https://elyobelyob.github.io/freely_tv_guide/channels/27936.json) |
| 23152 | That's TV 2 | [channels/23152.json](https://elyobelyob.github.io/freely_tv_guide/channels/23152.json) |
| 8700 | That's TV 3 | [channels/8700.json](https://elyobelyob.github.io/freely_tv_guide/channels/8700.json) |
| 37696 | The Chase | [channels/37696.json](https://elyobelyob.github.io/freely_tv_guide/channels/37696.json) |
| 16280 | Tiny Pop | [channels/16280.json](https://elyobelyob.github.io/freely_tv_guide/channels/16280.json) |
| 16364 | Together TV | [channels/16364.json](https://elyobelyob.github.io/freely_tv_guide/channels/16364.json) |
| 27808 | Together TV IP | [channels/27808.json](https://elyobelyob.github.io/freely_tv_guide/channels/27808.json) |
| 27392 | Together TV+1 | [channels/27392.json](https://elyobelyob.github.io/freely_tv_guide/channels/27392.json) |
| 16188 | Trailblazer | [channels/16188.json](https://elyobelyob.github.io/freely_tv_guide/channels/16188.json) |
| 22272 | U&Dave | [channels/22272.json](https://elyobelyob.github.io/freely_tv_guide/channels/22272.json) |
| 13008 | U&DaveJaVu | [channels/13008.json](https://elyobelyob.github.io/freely_tv_guide/channels/13008.json) |
| 16208 | U&Drama | [channels/16208.json](https://elyobelyob.github.io/freely_tv_guide/channels/16208.json) |
| 22336 | U&Drama+1 | [channels/22336.json](https://elyobelyob.github.io/freely_tv_guide/channels/22336.json) |
| 19968 | U&Eden | [channels/19968.json](https://elyobelyob.github.io/freely_tv_guide/channels/19968.json) |
| 28096 | U&W | [channels/28096.json](https://elyobelyob.github.io/freely_tv_guide/channels/28096.json) |
| 25792 | U&Yesterday | [channels/25792.json](https://elyobelyob.github.io/freely_tv_guide/channels/25792.json) |
| 14464 | WildEarth | [channels/14464.json](https://elyobelyob.github.io/freely_tv_guide/channels/14464.json) |
| 34368 | YAAAS! | [channels/34368.json](https://elyobelyob.github.io/freely_tv_guide/channels/34368.json) |
| 22592 | talkSPORT | [channels/22592.json](https://elyobelyob.github.io/freely_tv_guide/channels/22592.json) |
| 34496 | wedotv Movies UK | [channels/34496.json](https://elyobelyob.github.io/freely_tv_guide/channels/34496.json) |
| 33920 | wedotv movies | [channels/33920.json](https://elyobelyob.github.io/freely_tv_guide/channels/33920.json) |
<!-- CHANNELS_END -->

## Stats
See counts of channels and images here:  
https://elyobelyob.github.io/freely_tv_guide/stats/
