
# # Querie para pegar zonas na AM < 5km2 

# ```sql 

# select *
# from (
# 	select 
# 			*,
# 			ST_Area(geometry::geography) / 1e6 as area_km2
# 	from 
# 		land
# 	where state_uf = 'AM'
# ) as sub
# where area_km2 < 5
# limit 10; 
# ``` 