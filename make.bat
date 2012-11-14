@if not exist org mkdir org
@cd src
@dir /s /b *.java > ../org/files.txt
@cd ..
@sed -r -e "s/.*/\"\0\"/" -e "s~\\~/~g" org/files.txt > org/srcfiles.txt
@del org\files.txt
javac -d . -classpath sqlite-jdbc-3.7.2 @org/srcfiles.txt
