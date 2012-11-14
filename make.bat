@if not exist org mkdir org
@cd src
@dir /s /b *.java > ../org/srcfiles.txt
@cd ..
javac -d . -classpath sqlite-jdbc-3.7.2 @org/srcfiles.txt
