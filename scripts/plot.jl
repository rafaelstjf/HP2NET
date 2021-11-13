using PhyloNetworks
using PhyloPlots
using RCall

function getnamemp(network)
    net_file = open(network, "r")
    for line in eachline(net_file)
        if length(line) > 0 && line[1] == '('
            close(net_file)
            return line
        end
    end
end
#---
function getnamempl(network)
    net_file = open(network, "r")
    line = first(eachline(net_file))
    net_string = ""
    for i in 1:length(line)
        net_string = string(net_string, line[i])
        if line[i] == ';'
            break
        end
    end
    close(net_file)
    return net_string
end
#---
function plotnet(network, network_method)
    net_string = ""
    imagefilename = ""
    if network_method == "MPL"
        imagefilename = string(replace(network, ".out" => ".svg"))
        net_string = getnamempl(network)
    elseif network_method == "MP"
        imagefilename = string(replace(network, ".nex" => ".svg"))
        net_string = getnamemp(network)
    end
    if network_method == "MPL" || network_method == "MP" && length(net_string) > 0
        net = readTopology(string(net_string))
        R"svg"(imagefilename, width=10, height=10) # starts image file
        R"par"(mar=[0,0,0,0]) # to reduce margins (no margins at all here)
        plot(net, :R, showEdgeLength=false, showGamma=false);
        R"dev.off()"; # wrap up and save image file
        R"pdf"(string(replace(imagefilename, ".svg" => ".pdf")), width=10, height=10) # starts image file
        R"par"(mar=[0,0,0,0]) # to reduce margins (no margins at all here)
        plot(net, :R, showEdgeLength=false, showGamma=false);
        R"dev.off()"; # wrap up and save image file
    end
end
#---
if length(ARGS) < 1
    println("Missing arguments!")
else
    println("Plotting network")
    #arg[1] = network file
    #arg[2] = SNAQ (MPL) or Phylonet (MP)
    networks = split(ARGS[1], ',')
    for net in networks
        println(net)
        if last(split(strip(net), '.')) == "out"
            plotnet(strip(net), "MPL") 
        else
            plotnet(strip(net), "MP") 
        end
    end
    
end
