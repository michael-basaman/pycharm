import math


def nether(i):
    return math.floor(i / 8.0)


def chunk(i):
    return math.floor(i / 16.0)


class Location():
    def __init__(self, name, overworld_x, overworld_z, nether_x, nether_z):
        self.name = name
        self.ox = overworld_x
        self.oz = overworld_z
        self.nx = nether_x
        self.nz = nether_z

        self.new_nx = nether(self.ox)
        self.new_nz = nether(self.oz)
        self.cnx = chunk(self.nx)
        self.cnz = chunk(self.nz)
        self.new_cnx = chunk(self.new_nx)
        self.new_cnz = chunk(self.new_nz)
        self.build_ox = 0
        self.build_oz = 0
        self.build_nx = 0
        self.build_nz = 0
        self.distance = 1000000
        self.nether_distance = 0

        self.load()

    def load(self):
        for temp_nx in range(self.nx - 50, self.nx + 50):
            for temp_nz in range(self.nz - 50, self.nz + 50):
                if (abs(chunk(self.nx) - chunk(temp_nx)) > 1.01 and abs(chunk(self.new_nx) - chunk(temp_nx)) > 1.01) \
                        or (abs(chunk(self.nz) - chunk(temp_nz)) > 1.01 and abs(chunk(self.new_nz) - chunk(temp_nz)) > 1.01):
                    temp_ox = temp_nx * 8
                    temp_oz = temp_nz * 8
                    temp_distance = abs(temp_ox - self.ox) + abs(temp_oz - self.oz)
                    if temp_distance < self.distance:
                        self.distance = temp_distance
                        self.build_ox = temp_ox
                        self.build_oz = temp_oz
                        self.build_nx = temp_nx
                        self.build_nz = temp_nz

        self.nether_distance = abs(self.build_nx - self.nx) + abs(self.build_nz - self.nz)

    def print(self):
        print(f"Name: {self.name}")
        print(f"Overworld: {self.ox}, {self.oz}")
        print(f"Current Nether: {self.nx}, {self.nz}")
        print(f"New Nether: {self.new_nx}, {self.new_nz}")
        print(f"Build Overworld: {self.build_ox}, {self.build_oz}")
        print(f"Build Nether: {self.build_nx}, {self.build_nz}")
        print(f"Distance: {self.distance}")
        print(f"Nether Distance: {self.nether_distance  }")
        print("")

locations = []

locations.append(Location("xp", 1055, -515, 131, -78))
locations.append(Location("beach", 782, -464, 102, -58))
locations.append(Location("swamp", 1287, 153, 166, 12))

for location in locations:
    location.print()