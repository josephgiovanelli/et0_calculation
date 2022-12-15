def interpolate_group(group_key, group_df):
            """
            Applies interpolation to grouped (by installation) records.
            @param group_key: grouping key
            @param group_df: grouped records
            @return: a dataframe containing interpolated records
            """
            # Select just the columns of interest
            interpolated_group_df = group_df.copy()
            interpolated_group_df = interpolated_group_df[['xx', 'yy', 'zz', field_value]]
            # Mean the values if there are more values per coordinate
            interpolated_group_df = interpolated_group_df.groupby(['xx', 'yy', 'zz']).mean()
            interpolated_group_df = interpolated_group_df.reset_index()
            # Select just the vertices that compose an hyper-plane
            # interpolated_group_df = filter_vertices(interpolated_group_df)
            if group_key[2] == 'Fondo ZANNONI':
                value_80_20 = float(interpolated_group_df[(interpolated_group_df['xx'] == 80.) & (interpolated_group_df['yy'] == 20.)]['value'])
                value_80_60 = float(interpolated_group_df[(interpolated_group_df['xx'] == 80.) & (interpolated_group_df['yy'] == 60.)]['value'])
                interpolated_group_df = interpolated_group_df.append({
                    'xx': 80., 
                    'yy': 40., 
                    'zz': 0., 
                    'value': (value_80_20 + value_80_60)/2}, ignore_index=True)
            interpolated_group_df = interpolated_group_df.sort_values(by=['xx', 'yy', 'zz'])
            # Understand which interpolation to apply (1D, 2D, 3D)
            x = interpolated_group_df["xx"].unique()
            y = interpolated_group_df["yy"].unique()
            z = interpolated_group_df["zz"].unique()
            # Instantiate and populate the data structure where all the known coordinates are stored
            points = []
            if len(x) > 1:
                points += [x]
            if len(y) > 1:
                points += [y]
            if len(z) > 1:
                points += [z]
            # Instantiate the data structure where all the values of the known points are stored
            if len(points) == 1:
                values = np.zeros(len(points[0]))
            if len(points) == 2:
                values = np.zeros((len(points[0]), len(points[1])))
            if len(points) == 3:
                values = np.zeros((len(points[0]), len(points[1]), len(points[2])))
            # Populate the data structure where all the values of the known points are stored
            
            for i in range(x.shape[-1]):
                for j in range(y.shape[-1]):
                    for k in range(z.shape[-1]):
                        value = interpolated_group_df[(interpolated_group_df["xx"] == x[i]) & (interpolated_group_df["yy"] == y[j]) & (interpolated_group_df["zz"] == z[k])]["value"]
                        if len(points) == 1:
                            # Even though we do not know which is the coordinate that has not one unique value,
                            # we can sum all of the coordinates because the index of the ones that has just one unique value would be 0
                            values[i + j + k] = value
                        if len(points) == 2:
                            # We understand which is the pair of coordinates to consider, based on the one to not consider (lenght = 1)
                            if len(x) == 1:
                                values[j, k] = value
                            if len(y) == 1:
                                values[i, k] = value
                            if len(z) == 1:
                                values[i, j] = value
                        if len(points) == 3:
                            values[i, j, k] = value
            
            # Calculate interpolated vales
            for i in range(int(min_XX), int(max_XX + XX_step), int(XX_step)):
                for j in range(int(min_YY), int(max_YY + YY_step), int(YY_step)):
                    for k in range(int(min_ZZ), int(max_ZZ + ZZ_step), int(ZZ_step)):
                        if len(points) == 1:
                            point = np.array([i + j + k])
                        if len(points) == 2:
                            if len(x) == 1:
                                point = np.array([j, k])
                            if len(y) == 1:
                                point = np.array([i, k])
                            if len(z) == 1:
                                point = np.array([i, j])
                        if len(points) == 3:
                            point = np.array([i, j, k])
                        value = interpn(points, values, point)
                        if not(((interpolated_group_df['xx'] == i) & (interpolated_group_df['yy'] == j) & (interpolated_group_df['zz'] == k)).any()):
                            interpolated_group_df = interpolated_group_df.append(pd.DataFrame({"xx": [float(i)], "yy": [float(j)], "zz": [float(k)], field_value: [float(value)]}), ignore_index=True)

            return interpolated_group_df