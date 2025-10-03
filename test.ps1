# Testing Labs

# Check if Java is installed
try {
    java >$null 2>&1
}
catch {
    Write-Host "Java is not installed!"
    Exit 1
}

# Check Lab number
if (1..4 -notcontains $args[0]) {
    Write-Host "Wrong Lab number, use only 1 - 4 numbers!"
    Exit 1
}

# Run grading system
java -jar ".\dfs-grading-0.10\bin\grading-test-0.9.jar" "dfs-test-0.10.jar" "dfs.tests.Lab$($args[0])"
