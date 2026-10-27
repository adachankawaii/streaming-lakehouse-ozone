param(
  [string]$Path = "ofs://om/s3v/warehouse",
  [string]$Service = "om"
)

$raw = docker compose exec -T $Service ozone fs -ls -R $Path 2>$null
$entries = @()

foreach ($line in $raw) {
  if ($line -match '(ofs://\S+)$') {
    $entries += [PSCustomObject]@{
      FullPath = $Matches[1]
      IsDir = $line.StartsWith("d")
    }
  }
}

$root = $Path.TrimEnd('/')
$rootName = ($root -split '/')[-1]
Write-Output "$rootName/"

$prefix = "$root/"
$children = @{}

foreach ($e in $entries) {
  if (-not $e.FullPath.StartsWith($prefix)) { continue }
  $rel = $e.FullPath.Substring($prefix.Length)
  if (-not $rel) { continue }

  $parts = $rel -split '/'
  $parent = ""

  for ($i = 0; $i -lt $parts.Count; $i++) {
    $node = if ($parent) { "$parent/$($parts[$i])" } else { $parts[$i] }

    if (-not $children.ContainsKey($parent)) {
      $children[$parent] = [System.Collections.Generic.List[string]]::new()
    }
    if (-not $children[$parent].Contains($node)) {
      $children[$parent].Add($node)
    }

    $parent = $node
  }
}

function Show-Tree([string]$Parent="", [string]$Indent="") {
  if (-not $children.ContainsKey($Parent)) { return }

  $list = @($children[$Parent] | Sort-Object)
  for ($i = 0; $i -lt $list.Count; $i++) {
    $node = $list[$i]
    $last = ($i -eq $list.Count - 1)
    $branch = if ($last) { "└── " } else { "├── " }
    $name = ($node -split '/')[-1]
    if ($children.ContainsKey($node)) { $name += "/" }

    Write-Output "$Indent$branch$name"
    $nextIndent = $Indent + $(if ($last) { "    " } else { "│   " })
    Show-Tree $node $nextIndent
  }
}

Show-Tree
