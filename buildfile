VERSION_NUMBER = "1.0.0"
# Group identifier for your projects
GROUP = "nodeinfo"
COPYRIGHT = "Jeff Hodges <jeff@somethingsimilar.com>"

repositories.remote << "http://www.ibiblio.org/maven2/"

desc "The Nodeinfo project"
define "nodeinfo" do

  project.version = VERSION_NUMBER
  project.group = GROUP
  manifest["Implementation-Vendor"] = COPYRIGHT

  compile.dependencies << FileList['lib/*.jar']

  package(:jar)
end
