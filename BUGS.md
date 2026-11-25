# BUGS

1. **El perfil se limitaba a cinco países**
   - Reproducción: ejecutar el perfil con el recorte original de `focus_countries`.
   - Efecto: countries, languages e indicadores excluían el resto del universo.
   - Arreglo: cargar el universo desde `country_list.csv` y filtrar únicamente entidades válidas.

2. **Las respuestas HTTP erróneas podían tratarse como datos**
   - Reproducción: una URL devolvía una respuesta 500.
   - Efecto: el contenido de error podía escribirse en `data/raw/`.
   - Arreglo: guardar solamente HTTP 200 y registrar los estados distintos como fallos.

3. **Las comparaciones regionales mezclaban años**
   - Reproducción: comparar indicadores regionales sin fijar el año.
   - Efecto: la media o mediana podía combinar series de décadas distintas.
   - Arreglo: calcular la mediana regional filtrando el mismo `year` que el país observado.
