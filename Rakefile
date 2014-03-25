require 'neography'
require 'neography/tasks'
 
namespace :neo4j do
  task :create do
    neo = Neography::Rest.new
    nodes = {}
    5.times do |x|
      5.times do |y|
        node = neo.create_node({:x => x.to_f, :y=> y.to_f}) 
        nodes["#{x}-#{y}"] = node["self"].split('/').last
      end
    end
     
    4.times do |x|
      4.times do |y|
        neo.create_relationship("next_to", 
                                nodes["#{x}-#{y}"], 
                                nodes["#{x+1}-#{y}"], 
                                {:time => (1 + rand(4)).to_f }) 
        neo.create_relationship("next_to", 
                                nodes["#{x}-#{y}"], 
                                nodes["#{x}-#{y+1}"], 
                                {:time => (1 + rand(4)).to_f }) 
      end
      neo.create_relationship("next_to", 
                              nodes["#{x}-4"], 
                              nodes["#{x+1}-4"], 
                              {:time => (1 + rand(4)).to_f }) 
      neo.create_relationship("next_to", 
                              nodes["4-#{x}"], 
                              nodes["4-#{x+1}"], 
                              {:time => (1 + rand(4)).to_f }) 
    end
         
  end

  task :repackage do
    Rake::Task["neo4j:stop"].execute

    system("mvn clean package")
    system("cp -v target/*jar neo4j/plugins/")
    
    Rake::Task["neo4j:start"].execute
  end

end
