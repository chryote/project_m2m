Output docs/assets/pipeline_demo.gif

Set Theme "Catppuccin Mocha"
Set FontSize 13
Set Width 900
Set Height 620
Set Padding 18
Set Framerate 14
Set WindowBar Colorful
Set TypingSpeed 40ms

Type ".\build\financial_pipeline_benchmark.exe --gb 0.05"
Sleep 400ms
Enter

# Wait for CRC32C, Serialization, and gRPC Streaming benchmarks
Sleep 5s

# Pause on the completed results before looping
Sleep 3s
