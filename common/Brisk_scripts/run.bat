SET JRE_HOME="C:\Program Files\Java\jdk1.8.0_131"
SET arg_benchmark= -a WordCount --THz 450000 --sim --num_socket 8 --num_cpu 8 --monte -algo.st 5 -sit 5

set /a P=1
:loopP
set /a A=1
:loopA
set /a B=1
:loopB
for /l %%x in (1, 1, 1000) do (
%JRE_HOME%\bin\java.exe -jar C:\Users\tony\Documents\briskstream\BriskBenchamrks\target\briskstream-1.2.0-jar-with-dependencies.jar %arg_benchmark% -pt %P% -ct1 %A%  -ct2 %B% >> monte_%P%_%A%_%B%.txt
)

set /a "B = B * 2"
if %B% LEQ 32 goto loopB

set /a "A = A * 2"
if %A% LEQ 32 goto loopA

set /a "P = P * 2"
if %P% LEQ 32 goto loopP
